import streamlit as st
import pandas as pd
import graphviz
import os
import numpy as np
import re
import zipfile
import io

# Pagina configuratie
st.set_page_config(layout="wide")

# --- Data laad functie (uit ZIP) ---
@st.cache_data
def load_data_from_zip(zip_file_bytes):
    # (Functie ongewijzigd t.o.v. vorige ZIP versie)
    data = {}
    required_files = {
        "receptionists": "Receptionists.csv", "queues": "Queues.csv",
        "ringgroups": "ringgroups.csv", "users": "Users.csv",
        "trunks": "Trunks.csv",
    }
    all_files_found = True; loaded_files = []; missing_files = []
    try:
        with zipfile.ZipFile(io.BytesIO(zip_file_bytes), 'r') as zf:
            zip_base_filenames = {os.path.basename(f) for f in zf.namelist()}
            for key, filename in required_files.items():
                if filename in zip_base_filenames:
                    actual_zip_path = next((f for f in zf.namelist() if os.path.basename(f) == filename), None)
                    if actual_zip_path:
                        try:
                            try: df = pd.read_csv(zf.open(actual_zip_path), delimiter=";")
                            except: df = pd.read_csv(zf.open(actual_zip_path), delimiter=",")
                            data[key] = df; loaded_files.append(filename)
                        except Exception as e: st.error(f"Kon {filename} niet lezen uit ZIP: {e}"); all_files_found = False # Essentieel?
                else:
                    missing_files.append(filename);
                    if key in ["receptionists", "queues", "ringgroups", "users"]: all_files_found = False
        if not all_files_found: st.error(f"Essentiële bestanden missen: {', '.join(missing_files)}"); return None

        # Data Voorbereiding
        if "receptionists" in data and 'Virtual Extension Number' in data['receptionists'].columns:
            data['receptionists']['Virtual Extension Number'] = data['receptionists']['Virtual Extension Number'].astype(str)
            data["receptionists_primary"] = data["receptionists"][data["receptionists"]["Primair/Secundair"] == "Primair"].copy()
            data["receptionists_all"] = data["receptionists"].copy()
        else: data["receptionists_primary"], data["receptionists_all"] = pd.DataFrame(), pd.DataFrame()
        if "queues" in data and 'Virtual Extension Number' in data['queues'].columns: data['queues']['Virtual Extension Number'] = data['queues']['Virtual Extension Number'].astype(str)
        if "ringgroups" in data and 'Virtual Extension Number' in data['ringgroups'].columns: data['ringgroups']['Virtual Extension Number'] = data['ringgroups']['Virtual Extension Number'].astype(str)
        if "users" in data:
            users = data['users']
            if 'Number' in users.columns:
                try: users['Number'] = users['Number'].astype(str).str.replace(r'\.0$', '', regex=True)
                except: users['Number'] = users['Number'].astype(str)
            if 'Full Name' in users.columns: users['Naam'] = users['Full Name']
            elif 'Naam' not in users.columns and 'FirstName' in users.columns: users['Naam'] = users['FirstName'].fillna('') + ' ' + users['LastName'].fillna(''); users['Naam'] = users['Naam'].str.strip()
            data['users'] = users
        st.success(f"Succesvol geladen uit ZIP: {', '.join(loaded_files)}")
        return data
    except zipfile.BadZipFile: st.error("Ongeldig ZIP-bestand."); return None
    except Exception as e: st.error(f"Fout bij verwerken ZIP: {e}"); return None

# --- Helper Functies ---
def parse_destination(dest_string):
    """
    Parseert een bestemming string uit de CSV's naar type en identifier.
    Herkent nu ook formaten zoals "8020 QueueName".
    Returns: tuple: (type_hint, identifier) or (None, None)
    """
    if pd.isna(dest_string) or dest_string == "":
        return None, None
    dest_string = str(dest_string).strip()

    # 1. Check voor Type(Identifier ...) format (bv. Wachtrij(8020 ...))
    match_type_id = re.match(r"(\w+)\s?\(\s?(\d+).*", dest_string)
    if match_type_id:
        type_str = match_type_id.group(1).lower()
        identifier = match_type_id.group(2)
        if "wachtrij" in type_str or "queue" in type_str:
            return "Queue", identifier
        elif "belgroep" in type_str or "ringgroup" in type_str:
            return "RingGroup", identifier
        elif "gebruiker" in type_str or "user" in type_str or "extension" in type_str:
            return "User", identifier
        elif "digital" in type_str or "receptionist" in type_str or "ivr" in type_str:
            return "DR", identifier
        elif "voicemail" in type_str:
            return "Voicemail", identifier
        else:
            # Type onbekend, maar wel ID gevonden in dit format
            return "UnknownType", identifier

    # 2. Check voor Identifier Name format (bv. "8020 QueueName")
    #    Zoekt naar 3+ cijfers aan het begin, gevolgd door een spatie.
    match_id_name = re.match(r"^(\d{3,})\s+(.*)", dest_string)
    if match_id_name:
        identifier = match_id_name.group(1)
        # We weten het type niet zeker, get_node_label_and_style zoekt het uit
        return "ExtensionNumber", identifier

    # 3. Check voor simpele tekstuele commando's
    if dest_string.lower() == "end call":
        return "EndCall", "End Call"
    if dest_string.lower() == "repeat prompt":
        return "Repeat", "Repeat Prompt"
    if dest_string.lower() == "accept anyway":
        return "Accept", "Accept Anyway"

    # 4. Check voor extern nummer
    #    Staat toe dat er spaties in het nummer zitten na de +
    if dest_string.startswith("+") and dest_string[1:].replace(' ', '').isdigit():
        return "External", dest_string

    # 5. Check voor simpel extensie nummer (zonder naam erachter)
    if dest_string.isdigit():
        # We weten het type niet zeker
        return "ExtensionNumber", dest_string

    # Fallback voor onbekende tekst
    return "UnknownText", dest_string

def format_user_details(user_info):
    # (Ongewijzigd)
    details = [f"Ext: {str(user_info.get('Number', 'N/A')).replace('.0', '')}"]
    mob = user_info.get('MobileNumber', '');
    if pd.notna(mob) and str(mob).strip(): details.append(f"Mob: {str(mob).strip()}")
    cid = user_info.get('OutboundCallerID', '');
    if pd.notna(cid) and str(cid).strip(): details.append(f"CID: {str(cid).strip()}")
    did_str = user_info.get('DID', '');
    if pd.notna(did_str) and str(did_str).strip():
        first_did = str(did_str).split(':')[0].strip()
        if first_did: details.append(f"DID: {first_did}")
    return ", ".join(details)

def get_node_label_and_style(identifier, type_hint, all_data):
    """Genereert label en bepaalt stijl, nu inclusief Q/RG tijden (robuuster)."""
    users_df = all_data.get("users", pd.DataFrame())
    queues_df = all_data.get("queues", pd.DataFrame())
    ringgroups_df = all_data.get("ringgroups", pd.DataFrame())
    receptionists_df_all = all_data.get("receptionists_all", pd.DataFrame())

    label = f"❓ Onbekend ID: {identifier}"; shape = 'box'; fillcolor = 'lightgrey'; node_type = "Unknown"

    if pd.isna(identifier) or identifier == "": label = "Niet geconfigureerd"; node_type="ConfigError"
    elif type_hint == "EndCall": label = "❌ Ophangen"; shape='octagon'; fillcolor='red'; node_type="End"
    elif type_hint == "Repeat": label = "🔁 Herhaal Prompt"; shape='invhouse'; fillcolor='orange'; node_type="Action"
    elif type_hint == "External": label = f"📞 Extern:\n{identifier}"; shape='note'; fillcolor='khaki'; node_type="External"
    elif type_hint == "Accept": label = "➡️ Accepteer"; shape='rarrow'; fillcolor='lightgreen'; node_type="Action"
    elif type_hint == "UnknownText": label = f"❓ Tekst:\n{identifier}"; node_type="Unknown"
    elif str(identifier).isdigit():
        ext_nr = str(identifier); label = f"❓ Ext: {ext_nr}"

        # Check Queues
        if node_type=="Unknown" and not queues_df.empty and (type_hint=="Queue" or type_hint=="ExtensionNumber" or type_hint=="UnknownType"):
            queue = queues_df[queues_df["Virtual Extension Number"] == ext_nr]
            if not queue.empty:
                queue_info=queue.iloc[0]; queue_name=queue_info.get('Queue Name',f"Queue {ext_nr}")

                # --- Verbeterde Tijd Ophalen ---
                ring_time_str = "N/A"; max_wait_str = "N/A"
                # Ring time
                if 'Ring time (s)' in queue_info:
                    ring_time_val = pd.to_numeric(queue_info['Ring time (s)'], errors='coerce')
                    if pd.notna(ring_time_val):
                        ring_time_str = f"{int(ring_time_val)}s"
                # Max queue wait time
                if 'Max queue wait time (s)' in queue_info:
                     max_wait_val = pd.to_numeric(queue_info['Max queue wait time (s)'], errors='coerce')
                     if pd.notna(max_wait_val):
                          max_wait_str = f"{int(max_wait_val)}s"
                time_label = f"(Ring: {ring_time_str}, MaxWait: {max_wait_str})"
                # --- Einde Verbeterde Tijd Ophalen ---

                members = [f"{q_info[col]} ({format_user_details(u_info.iloc[0])})" if not (u_info := users_df[users_df['Naam'] == q_info[col]]).empty else f"{q_info[col]} (❓)" for col in queue_info.index if col.startswith("User ") and pd.notna(q_info:=queue_info)[col]]
                members_str = "\n ".join(members) if members else "(Geen leden)"
                label = f"👥 Queue: {queue_name} ({ext_nr})\n{time_label}\nLeden:\n {members_str}"; shape='box'; fillcolor='palegreen'; node_type="Queue"

        # Check Ring Groups
        if node_type=="Unknown" and not ringgroups_df.empty and (type_hint=="RingGroup" or type_hint=="ExtensionNumber" or type_hint=="UnknownType"):
            rg = ringgroups_df[ringgroups_df["Virtual Extension Number"] == ext_nr]
            if not rg.empty:
                rg_info=rg.iloc[0]; rg_name=rg_info.get('Ring Group Name',f"Ring Group {ext_nr}")

                # --- Verbeterde Tijd Ophalen ---
                ring_time_str = "N/A"
                if 'Ring time (s)' in rg_info:
                    ring_time_val = pd.to_numeric(rg_info['Ring time (s)'], errors='coerce')
                    if pd.notna(ring_time_val):
                        ring_time_str = f"{int(ring_time_val)}s"
                time_label = f"(Ring: {ring_time_str})"
                 # --- Einde Verbeterde Tijd Ophalen ---

                members = [f"{ri[col]} ({format_user_details(u_info.iloc[0])})" if not (u_info := users_df[users_df['Naam'] == ri[col]]).empty else f"{ri[col]} (❓)" for col in rg_info.index if col.startswith("User ") and pd.notna(ri:=rg_info)[col]]
                members_str = "\n ".join(members) if members else "(Geen leden)"
                label = f"🔔 RG: {rg_name} ({ext_nr})\n{time_label}\nLeden:\n {members_str}"; shape='box'; fillcolor='lightskyblue'; node_type="RingGroup"

        # Check Users (als geen queue/rg)
        if node_type == "Unknown" and not users_df.empty and (type_hint == "User" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
            user = users_df[users_df["Number"] == ext_nr]
            if not user.empty:
                user_info = user.iloc[0]; user_name = user_info.get('Naam', f"User {ext_nr}")
                label = f"👤 Gebruiker: {user_name}\n({format_user_details(user_info)})"; shape='ellipse'; fillcolor='whitesmoke'; node_type="User"

        # Check DRs (als geen queue/rg/user)
        if node_type == "Unknown" and not receptionists_df_all.empty and (type_hint == "DR" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
             dr = receptionists_df_all[receptionists_df_all["Virtual Extension Number"] == ext_nr]
             if not dr.empty: dr_info=dr.iloc[0]; dr_name=dr_info.get('Digital Receptionist Name',f"DR {ext_nr}"); label = f"🚦 IVR: {dr_name}\n({ext_nr})"; shape='Mdiamond'; fillcolor='lightcoral'; node_type="DR"

        # Check Voicemail (specifiek type)
        if node_type == "Unknown" and type_hint == "Voicemail":
            user_vm = users_df[users_df["Number"] == ext_nr] if not users_df.empty else pd.DataFrame()
            vm_owner = user_vm.iloc[0].get('Naam', '') if not user_vm.empty else ''
            label = f"🎙️ Voicemail ({ext_nr})\n{'van: '+vm_owner if vm_owner else ''}"; shape='cylinder'; fillcolor='mediumpurple'; node_type="Voicemail"

    return label, shape, fillcolor, node_type

# --- Streamlit UI & Hoofdlogica ---
st.title("📞 3CX Call Flow Visualizer (Per Onderdeel)")
st.markdown("Upload een **ZIP-bestand** met `Receptionists.csv`, `Queues.csv`, `ringgroups.csv`, `Users.csv`.")

uploaded_zip = st.file_uploader("Upload CSVs (ZIP)", type="zip")
all_data = None

if uploaded_zip is not None:
    zip_content_bytes = uploaded_zip.getvalue()
    all_data = load_data_from_zip(zip_content_bytes) # Functie van vorige versie

if all_data:
    # Gebruik alle receptionists, niet alleen primaire
    receptionists_df_all = all_data.get("receptionists_all", pd.DataFrame())
    queues_df = all_data.get("queues", pd.DataFrame())
    ringgroups_df = all_data.get("ringgroups", pd.DataFrame())
    users_df = all_data.get("users", pd.DataFrame()) # Users ophalen

    # --- Refactored Helper Functies (buiten loops) ---
    # Deze functies zijn nu beschikbaar voor zowel onderdeel- als individuele DR-flows.

    def make_node_id_refactored(prefix, identifier, context):
        """Genereert een unieke node ID met context (onderdeel of DR ext)."""
        safe_identifier = re.sub(r'\\W+', '_', str(identifier))
        safe_context = re.sub(r'\\W+', '_', str(context))
        return f"{prefix}_{safe_context}_{safe_identifier[:30]}"

    added_nodes_global = set() # Houdt nodes bij over meerdere grafieken indien nodig, of reset per grafiek.
    added_edges_global = set() # Idem voor edges.

    def create_or_get_node_refactored(dot_graph, node_id, label, added_nodes_set, shape='box', fillcolor='lightblue'):
         """Voegt een node toe aan de grafiek als deze nog niet bestaat."""
         if node_id not in added_nodes_set:
             dot_graph.node(node_id, label, shape=shape, fillcolor=fillcolor)
             added_nodes_set.add(node_id)
         return node_id

    def draw_destination_refactored(dot_graph, source_node_id, edge_label, dest_string, current_all_data, context, added_nodes_set, added_edges_set):
        """Tekent een pijl naar een bestemming, inclusief 'no answer' routes."""
        dest_type, dest_id = parse_destination(dest_string)
        # Gebruik de gerefactorde make_node_id met context
        target_node_id = make_node_id_refactored("END", f"{source_node_id}_{edge_label.replace(' ','_').replace('/','_')}", context)

        target_label, target_shape, target_color, target_node_type = get_node_label_and_style(None, "EndCall", current_all_data)
        if dest_type:
            target_node_id = make_node_id_refactored(f"DEST_{dest_type}", f"{dest_id}_{edge_label.replace(' ','_').replace('/','_')}", context)
            target_label, target_shape, target_color, target_node_type = get_node_label_and_style(dest_id, dest_type, current_all_data)

        # Gebruik de gerefactorde create_or_get_node
        create_or_get_node_refactored(dot_graph, target_node_id, target_label, added_nodes_set, shape=target_shape, fillcolor=target_color)

        edge_key = (source_node_id, target_node_id, edge_label)
        if edge_key not in added_edges_set:
            dot_graph.edge(source_node_id, target_node_id, label=edge_label)
            added_edges_set.add(edge_key)

        if target_node_type in ["Queue", "RingGroup"] and dest_id:
             q_df = current_all_data.get("queues", pd.DataFrame())
             rg_df = current_all_data.get("ringgroups", pd.DataFrame())
             df_lookup = q_df if target_node_type == "Queue" else rg_df
             row = df_lookup[df_lookup["Virtual Extension Number"] == str(dest_id)]
             if not row.empty:
                 noans_dest = row.iloc[0].get("Destination if no answer", np.nan)
                 noans_type, noans_id = parse_destination(noans_dest)
                 noans_target_node_id = make_node_id_refactored("END", f"{target_node_id}_NoAns", context)
                 noans_label, noans_shape, noans_color, _ = get_node_label_and_style(None, "EndCall", current_all_data)
                 if noans_type:
                      noans_target_node_id = make_node_id_refactored(f"NOANS_{noans_type}", f"{target_node_id}_{noans_id}", context)
                      noans_label, noans_shape, noans_color, _ = get_node_label_and_style(noans_id, noans_type, current_all_data)
                 create_or_get_node_refactored(dot_graph, noans_target_node_id, noans_label, added_nodes_set, shape=noans_shape, fillcolor=noans_color)
                 noans_edge_key = (target_node_id, noans_target_node_id, "No Answer")
                 if noans_edge_key not in added_edges_set:
                     dot_graph.edge(target_node_id, noans_target_node_id, label="No Answer")
                     added_edges_set.add(noans_edge_key)

    # --- Creëer tabs ---
    tab1, tab2, tab3 = st.tabs([
        "📊 Flows per Onderdeel",
        "👥 Users per Onderdeel",
        "👤 DRs per User"
    ])

    # --- Tab 1: Flows per Onderdeel / Individuele DR ---
    with tab1:
        st.header("Call Flows per Onderdeel")
        st.write("Klik op een Onderdeel om de gegroepeerde flow uit te klappen.")

        if receptionists_df_all.empty:
            st.warning("Geen Digital Receptionists gevonden in het ZIP-bestand.")
        elif 'Onderdeel' not in receptionists_df_all.columns:
            st.error("Kolom 'Onderdeel' niet gevonden in Receptionists.csv. Kan flows niet groeperen.")
        else:
            # Maak kolom 'Onderdeel' string en vul NaN
            receptionists_df_all['Onderdeel'] = receptionists_df_all['Onderdeel'].astype(str).fillna('LEEG')
            # Filter DRs met een geldig onderdeel (niet LEEG en niet '?')
            drs_met_geldig_onderdeel = receptionists_df_all[
                (receptionists_df_all['Onderdeel'] != 'LEEG') &
                (receptionists_df_all['Onderdeel'] != '?') &
                (receptionists_df_all['Onderdeel'].str.strip() != '') # Ook lege strings uitsluiten
            ].copy()
            # Filter DRs zonder geldig onderdeel
            drs_zonder_geldig_onderdeel = receptionists_df_all[
                (receptionists_df_all['Onderdeel'] == 'LEEG') |
                (receptionists_df_all['Onderdeel'] == '?') |
                (receptionists_df_all['Onderdeel'].str.strip() == '')
            ].copy()

            # --- 1. Genereer Flows per Geldig Onderdeel ---
            if not drs_met_geldig_onderdeel.empty:
                grouped_receptionists = drs_met_geldig_onderdeel.groupby('Onderdeel')
                for onderdeel_naam, onderdeel_group_df in grouped_receptionists:
                    onderdeel_safe_name = re.sub(r'\\W+', '_', onderdeel_naam)

                    with st.expander(f"Onderdeel: {onderdeel_naam}"):
                        dot_onderdeel = graphviz.Digraph(name=f'Flow_Onderdeel_{onderdeel_safe_name}', comment=f'Call Flow for Onderdeel {onderdeel_naam}')
                        dot_onderdeel.attr(rankdir='LR', size='25,25!', ranksep='0.8', nodesep='0.6', overlap='prism', splines='spline')
                        dot_onderdeel.attr('node', shape='box', style='rounded,filled', fontname='Arial', fontsize='9')
                        dot_onderdeel.attr('edge', fontname='Arial', fontsize='8')
                        added_nodes_onderdeel = set() # Reset voor deze grafiek
                        added_edges_onderdeel = set()

                        # --- Teken de flow voor het Onderdeel (gebruik refactored helpers) ---
                        onderdeel_node_id = make_node_id_refactored("ONDERDEEL", onderdeel_safe_name, onderdeel_safe_name)
                        create_or_get_node_refactored(dot_onderdeel, onderdeel_node_id, f"🏢 Onderdeel:\\n{onderdeel_naam}", added_nodes_onderdeel, shape='tab', fillcolor='lightblue')

                        primaire_drs_in_onderdeel = onderdeel_group_df[onderdeel_group_df['Primair/Secundair'] == 'Primair']
                        start_drs_df = primaire_drs_in_onderdeel
                        start_label_prefix = "Start bij Primaire DR:"
                        if primaire_drs_in_onderdeel.empty:
                            start_drs_df = onderdeel_group_df
                            start_label_prefix = "Start bij DR:"

                        if start_drs_df.empty:
                             st.warning(f"Geen DRs gevonden voor onderdeel '{onderdeel_naam}' in deze groep.")
                             continue

                        for _, dr in start_drs_df.iterrows():
                            dr_name = dr.get("Digital Receptionist Name", "Naamloos")
                            dr_ext = dr.get("Virtual Extension Number", "GEEN_EXT")
                            if dr_ext == "GEEN_EXT" or pd.isna(dr_ext): continue
                            dr_ext_str = str(dr_ext)
                            context_id = f"{onderdeel_safe_name}_{dr_ext_str}" # Context voor nodes binnen deze DR flow

                            menu_options_strings = {}
                            has_menu = False
                            for i in range(10):
                                menu_col = f"Menu {i}"
                                if menu_col in dr.index and pd.notna(dr[menu_col]) and str(dr[menu_col]).strip():
                                    menu_options_strings[i] = dr[menu_col]
                                    has_menu = True

                            ivr_timeout_sec_val = dr.get("If no input within seconds", None)
                            ivr_timeout_num = pd.to_numeric(ivr_timeout_sec_val, errors='coerce')
                            ivr_timeout_info = ""
                            if has_menu and pd.notna(ivr_timeout_num):
                                ivr_timeout_info = f"\\nTimeout: {int(ivr_timeout_num)}s"

                            dr_node_id = make_node_id_refactored("DR", dr_ext_str, context_id)
                            dr_label = f"🚦 IVR: {dr_name}\\n({dr_ext_str}){ivr_timeout_info}"
                            _, dr_shape, dr_color, _ = get_node_label_and_style(dr_ext_str, "DR", all_data)
                            create_or_get_node_refactored(dot_onderdeel, dr_node_id, dr_label, added_nodes_onderdeel, shape=dr_shape, fillcolor=dr_color)

                            edge_key = (onderdeel_node_id, dr_node_id, start_label_prefix)
                            if edge_key not in added_edges_onderdeel:
                                 dot_onderdeel.edge(onderdeel_node_id, dr_node_id, label=start_label_prefix)
                                 added_edges_onderdeel.add(edge_key)

                            office_check_node_id = make_node_id_refactored("OFFICECHECK", dr_ext_str, context_id)
                            _, office_shape, office_color, _ = get_node_label_and_style("", "Check", all_data)
                            create_or_get_node_refactored(dot_onderdeel, office_check_node_id, "Binnen kantooruren?", added_nodes_onderdeel, shape=office_shape, fillcolor=office_color)
                            edge_key = (dr_node_id, office_check_node_id)
                            if edge_key not in added_edges_onderdeel: dot_onderdeel.edge(dr_node_id, office_check_node_id); added_edges_onderdeel.add(edge_key)

                            break_check_node_id = make_node_id_refactored("BREAKCHECK", dr_ext_str, context_id)
                            create_or_get_node_refactored(dot_onderdeel, break_check_node_id, "Pauze actief?", added_nodes_onderdeel, shape=office_shape, fillcolor=office_color)
                            edge_key = (office_check_node_id, break_check_node_id, "Ja")
                            if edge_key not in added_edges_onderdeel: dot_onderdeel.edge(office_check_node_id, break_check_node_id, label="Ja"); added_edges_onderdeel.add(edge_key)

                            holiday_check_node_id = make_node_id_refactored("HOLIDAYCHECK", dr_ext_str, context_id)
                            create_or_get_node_refactored(dot_onderdeel, holiday_check_node_id, "Vakantie actief?", added_nodes_onderdeel, shape=office_shape, fillcolor=office_color)
                            edge_key = (break_check_node_id, holiday_check_node_id, "Nee")
                            if edge_key not in added_edges_onderdeel: dot_onderdeel.edge(break_check_node_id, holiday_check_node_id, label="Nee"); added_edges_onderdeel.add(edge_key)

                            in_hours_node_id = make_node_id_refactored("INHOURS", dr_ext_str, context_id)
                            create_or_get_node_refactored(dot_onderdeel, in_hours_node_id, "Actie binnen kantooruren", added_nodes_onderdeel, shape='ellipse', fillcolor='lightgrey')
                            edge_key = (holiday_check_node_id, in_hours_node_id, "Nee")
                            if edge_key not in added_edges_onderdeel: dot_onderdeel.edge(holiday_check_node_id, in_hours_node_id, label="Nee"); added_edges_onderdeel.add(edge_key)

                            dest_strings = {
                                'closed': dr.get("When office is closed route to", np.nan),
                                'break': dr.get("When on break route to", np.nan),
                                'holiday': dr.get(next((col for col in ["When on holiday route to", "When on holiday route to "] if col in dr.index), "non_existing_col"), np.nan),
                                'default': dr.get("Send call to", np.nan),
                                'invalid': dr.get("Invalid input destination", np.nan)
                            }
                            # Gebruik refactored draw_destination met context_id
                            draw_destination_refactored(dot_onderdeel, office_check_node_id, "Nee", dest_strings['closed'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)
                            draw_destination_refactored(dot_onderdeel, break_check_node_id, "Ja", dest_strings['break'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)
                            holiday_type, _ = parse_destination(dest_strings['holiday'])
                            if holiday_type:
                                draw_destination_refactored(dot_onderdeel, holiday_check_node_id, "Ja", dest_strings['holiday'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)
                            else:
                                edge_key = (holiday_check_node_id, in_hours_node_id, "Ja (geen route)")
                                if edge_key not in added_edges_onderdeel:
                                    dot_onderdeel.edge(holiday_check_node_id, in_hours_node_id, label="Ja (geen route)")
                                    added_edges_onderdeel.add(edge_key)

                            ivr_timeout_edge_label_part = f" ({int(ivr_timeout_num)}s)" if pd.notna(ivr_timeout_num) else ""

                            if has_menu:
                                create_or_get_node_refactored(dot_onderdeel, in_hours_node_id, "🎶 Menu speelt...", added_nodes_onderdeel) # Update label if it exists
                                for key, dest_str in menu_options_strings.items():
                                    draw_destination_refactored(dot_onderdeel, in_hours_node_id, f"Kies {key}", dest_str, all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)

                                timeout_edge_label = f"Timeout{ivr_timeout_edge_label_part} /\\nGeen invoer"
                                draw_destination_refactored(dot_onderdeel, in_hours_node_id, timeout_edge_label, dest_strings['default'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)

                                if pd.notna(dest_strings['invalid']) and dest_strings['invalid'] != dest_strings['default']:
                                     draw_destination_refactored(dot_onderdeel, in_hours_node_id, "Invalid", dest_strings['invalid'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)

                            else: # Geen menu
                                create_or_get_node_refactored(dot_onderdeel, in_hours_node_id, "Geen menu", added_nodes_onderdeel)
                                draw_destination_refactored(dot_onderdeel, in_hours_node_id, "Direct", dest_strings['default'], all_data, context_id, added_nodes_onderdeel, added_edges_onderdeel)

                        # Toon grafiek voor dit onderdeel
                        try:
                            st.graphviz_chart(dot_onderdeel, use_container_width=True)
                        except Exception as e:
                            st.error(f"Fout genereren grafiek voor onderdeel '{onderdeel_naam}': {e}")
                            st.code(dot_onderdeel.source, language='dot')
            else:
                st.info("Geen Digital Receptionists met een geldig Onderdeel gevonden.")

            # --- 2. Genereer Flows per Individuele DR (zonder geldig onderdeel) ---
            if not drs_zonder_geldig_onderdeel.empty:
                st.divider()
                st.header("Individuele Call Flows (Geen/Ongeldig Onderdeel)")
                st.write("Flows voor Digital Receptionists zonder specifiek onderdeel of met '?' als onderdeel.")

                for _, dr in drs_zonder_geldig_onderdeel.iterrows():
                    dr_name = dr.get("Digital Receptionist Name", "Naamloos")
                    dr_ext = dr.get("Virtual Extension Number", "GEEN_EXT")
                    if dr_ext == "GEEN_EXT" or pd.isna(dr_ext): continue
                    dr_ext_str = str(dr_ext)
                    context_id = dr_ext_str # Context is de DR extensie zelf

                    with st.expander(f"Individuele IVR: {dr_name} ({dr_ext_str})"):
                        dot_individual = graphviz.Digraph(name=f'Flow_Indiv_{dr_ext_str}', comment=f'Individual Call Flow for {dr_name}')
                        dot_individual.attr(rankdir='LR', size='25,25!', ranksep='0.8', nodesep='0.6', overlap='prism', splines='spline')
                        dot_individual.attr('node', shape='box', style='rounded,filled', fontname='Arial', fontsize='9')
                        dot_individual.attr('edge', fontname='Arial', fontsize='8')
                        added_nodes_indiv = set() # Reset voor deze grafiek
                        added_edges_indiv = set()

                        # --- Teken flow voor deze individuele DR (vergelijkbaar met onderdeel-loop, maar start bij DR) ---
                        menu_options_strings = {}
                        has_menu = False
                        for i in range(10):
                            menu_col = f"Menu {i}"
                            if menu_col in dr.index and pd.notna(dr[menu_col]) and str(dr[menu_col]).strip():
                                menu_options_strings[i] = dr[menu_col]
                                has_menu = True

                        ivr_timeout_sec_val = dr.get("If no input within seconds", None)
                        ivr_timeout_num = pd.to_numeric(ivr_timeout_sec_val, errors='coerce')
                        ivr_timeout_info = ""
                        if has_menu and pd.notna(ivr_timeout_num):
                            ivr_timeout_info = f"\\nTimeout: {int(ivr_timeout_num)}s"

                        # Start node is de DR zelf
                        dr_node_id = make_node_id_refactored("DR", dr_ext_str, context_id)
                        dr_label = f"🚦 IVR: {dr_name}\\n({dr_ext_str}){ivr_timeout_info}"
                        _, dr_shape, dr_color, _ = get_node_label_and_style(dr_ext_str, "DR", all_data)
                        create_or_get_node_refactored(dot_individual, dr_node_id, dr_label, added_nodes_indiv, shape=dr_shape, fillcolor=dr_color)

                        # Tijd checks (direct gekoppeld aan DR node)
                        office_check_node_id = make_node_id_refactored("OFFICECHECK", dr_ext_str, context_id)
                        _, office_shape, office_color, _ = get_node_label_and_style("", "Check", all_data)
                        create_or_get_node_refactored(dot_individual, office_check_node_id, "Binnen kantooruren?", added_nodes_indiv, shape=office_shape, fillcolor=office_color)
                        edge_key = (dr_node_id, office_check_node_id)
                        if edge_key not in added_edges_indiv: dot_individual.edge(dr_node_id, office_check_node_id); added_edges_indiv.add(edge_key)

                        break_check_node_id = make_node_id_refactored("BREAKCHECK", dr_ext_str, context_id)
                        create_or_get_node_refactored(dot_individual, break_check_node_id, "Pauze actief?", added_nodes_indiv, shape=office_shape, fillcolor=office_color)
                        edge_key = (office_check_node_id, break_check_node_id, "Ja")
                        if edge_key not in added_edges_indiv: dot_individual.edge(office_check_node_id, break_check_node_id, label="Ja"); added_edges_indiv.add(edge_key)

                        holiday_check_node_id = make_node_id_refactored("HOLIDAYCHECK", dr_ext_str, context_id)
                        create_or_get_node_refactored(dot_individual, holiday_check_node_id, "Vakantie actief?", added_nodes_indiv, shape=office_shape, fillcolor=office_color)
                        edge_key = (break_check_node_id, holiday_check_node_id, "Nee")
                        if edge_key not in added_edges_indiv: dot_individual.edge(break_check_node_id, holiday_check_node_id, label="Nee"); added_edges_indiv.add(edge_key)

                        in_hours_node_id = make_node_id_refactored("INHOURS", dr_ext_str, context_id)
                        create_or_get_node_refactored(dot_individual, in_hours_node_id, "Actie binnen kantooruren", added_nodes_indiv, shape='ellipse', fillcolor='lightgrey')
                        edge_key = (holiday_check_node_id, in_hours_node_id, "Nee")
                        if edge_key not in added_edges_indiv: dot_individual.edge(holiday_check_node_id, in_hours_node_id, label="Nee"); added_edges_indiv.add(edge_key)

                        # Bestemmingen en routes (zelfde logica als voorheen, gebruik refactored helpers)
                        dest_strings = {
                            'closed': dr.get("When office is closed route to", np.nan),
                            'break': dr.get("When on break route to", np.nan),
                            'holiday': dr.get(next((col for col in ["When on holiday route to", "When on holiday route to "] if col in dr.index), "non_existing_col"), np.nan),
                            'default': dr.get("Send call to", np.nan),
                            'invalid': dr.get("Invalid input destination", np.nan)
                        }
                        draw_destination_refactored(dot_individual, office_check_node_id, "Nee", dest_strings['closed'], all_data, context_id, added_nodes_indiv, added_edges_indiv)
                        draw_destination_refactored(dot_individual, break_check_node_id, "Ja", dest_strings['break'], all_data, context_id, added_nodes_indiv, added_edges_indiv)
                        holiday_type, _ = parse_destination(dest_strings['holiday'])
                        if holiday_type:
                            draw_destination_refactored(dot_individual, holiday_check_node_id, "Ja", dest_strings['holiday'], all_data, context_id, added_nodes_indiv, added_edges_indiv)
                        else:
                            edge_key = (holiday_check_node_id, in_hours_node_id, "Ja (geen route)")
                            if edge_key not in added_edges_indiv:
                                dot_individual.edge(holiday_check_node_id, in_hours_node_id, label="Ja (geen route)")
                                added_edges_indiv.add(edge_key)

                        ivr_timeout_edge_label_part = f" ({int(ivr_timeout_num)}s)" if pd.notna(ivr_timeout_num) else ""

                        if has_menu:
                            create_or_get_node_refactored(dot_individual, in_hours_node_id, "🎶 Menu speelt...", added_nodes_indiv)
                            for key, dest_str in menu_options_strings.items():
                                draw_destination_refactored(dot_individual, in_hours_node_id, f"Kies {key}", dest_str, all_data, context_id, added_nodes_indiv, added_edges_indiv)

                            timeout_edge_label = f"Timeout{ivr_timeout_edge_label_part} /\\nGeen invoer"
                            draw_destination_refactored(dot_individual, in_hours_node_id, timeout_edge_label, dest_strings['default'], all_data, context_id, added_nodes_indiv, added_edges_indiv)

                            if pd.notna(dest_strings['invalid']) and dest_strings['invalid'] != dest_strings['default']:
                                 draw_destination_refactored(dot_individual, in_hours_node_id, "Invalid", dest_strings['invalid'], all_data, context_id, added_nodes_indiv, added_edges_indiv)

                        else: # Geen menu
                            create_or_get_node_refactored(dot_individual, in_hours_node_id, "Geen menu", added_nodes_indiv)
                            draw_destination_refactored(dot_individual, in_hours_node_id, "Direct", dest_strings['default'], all_data, context_id, added_nodes_indiv, added_edges_indiv)

                        # Toon grafiek voor deze individuele DR
                        try:
                            st.graphviz_chart(dot_individual, use_container_width=True)
                        except Exception as e:
                            st.error(f"Fout genereren grafiek voor IVR '{dr_name}' ({dr_ext_str}): {e}")
                            st.code(dot_individual.source, language='dot')
            elif drs_met_geldig_onderdeel.empty:
                 st.info("Geen Digital Receptionists zonder geldig Onderdeel gevonden om individueel weer te geven.")

    # --- Tab 2: Users per Onderdeel ---
    with tab2:
        st.header("Overzicht: Gebruikers bereikbaar per Onderdeel")

        # Controleer of benodigde dataframes bestaan
        if users_df.empty or receptionists_df_all.empty:
            st.warning("Bestanden 'Users.csv' of 'Receptionists.csv' ontbreken of zijn leeg.")
        elif 'Onderdeel' not in receptionists_df_all.columns or 'Number' not in users_df.columns or 'Department' not in users_df.columns:
             st.error("Benodigde kolommen ('Onderdeel', 'Number', 'Department', 'Naam') ontbreken in de CSV-bestanden.")
        else:
            # Helper functie om users te vinden (met cycle detection)
            @st.cache_data # Cache resultaten van deze complexe zoekactie
            def find_reachable_users(start_destination_string, _all_data, max_depth=10):
                users_found = set()
                queue = [(start_destination_string, 0)] # (destination, depth)
                visited_nodes = set() # Houd bij welke nodes (type, id) we bezoeken om cycli te voorkomen

                while queue:
                    current_dest_str, depth = queue.pop(0)
                    if depth > max_depth: continue # Voorkom oneindige lussen bij diepe recursie/cycli

                    dest_type, dest_id = parse_destination(current_dest_str)
                    node_key = (dest_type, dest_id)

                    if not dest_type or node_key in visited_nodes:
                        continue
                    visited_nodes.add(node_key)

                    # Zoek user info op nummer
                    if dest_type == "User" or dest_type == "ExtensionNumber" or dest_type == "UnknownType":
                        # Gebruik str(dest_id) voor consistente lookup
                        user_match = _all_data["users"][_all_data["users"]["Number"] == str(dest_id)]
                        if not user_match.empty:
                            user_info = user_match.iloc[0]
                            # --- Department Handling --- 
                            department = user_info.get('Department', 'Geen Afdeling')
                            if pd.isna(department) or str(department).strip() == "":
                                department = "Geen Afdeling"
                            # --- End Department Handling ---
                            users_found.add((
                                user_info.get('Number', dest_id),
                                user_info.get('Naam', f'User {dest_id}'),
                                str(department) # Zorg dat het een string is
                            ))
                            continue

                    # Check leden van Queues en Ring Groups en hun 'no answer' bestemming
                    if dest_type == "Queue" or (dest_type == "ExtensionNumber" and not _all_data["queues"][_all_data["queues"]["Virtual Extension Number"] == str(dest_id)].empty):
                         q_df = _all_data.get("queues", pd.DataFrame())
                         queue_match = q_df[q_df["Virtual Extension Number"] == str(dest_id)]
                         if not queue_match.empty:
                             queue_info = queue_match.iloc[0]
                             for col in queue_info.index:
                                 if col.startswith("User ") and pd.notna(queue_info[col]):
                                     user_name_in_queue = str(queue_info[col])
                                     # Zoek user op naam voor Department info
                                     user_match_by_name = _all_data["users"][_all_data["users"]["Naam"] == user_name_in_queue]
                                     if not user_match_by_name.empty:
                                          user_info = user_match_by_name.iloc[0]
                                          # --- Department Handling --- 
                                          department = user_info.get('Department', 'Geen Afdeling')
                                          if pd.isna(department) or str(department).strip() == "":
                                              department = "Geen Afdeling"
                                          # --- End Department Handling ---
                                          users_found.add((
                                                user_info.get('Number', 'N/A'),
                                                user_info.get('Naam', user_name_in_queue),
                                                str(department) # Zorg dat het een string is
                                          ))
                             noans_dest = queue_info.get("Destination if no answer", np.nan)
                             if pd.notna(noans_dest): queue.append((noans_dest, depth + 1))

                    elif dest_type == "RingGroup" or (dest_type == "ExtensionNumber" and not _all_data["ringgroups"][_all_data["ringgroups"]["Virtual Extension Number"] == str(dest_id)].empty):
                        rg_df = _all_data.get("ringgroups", pd.DataFrame())
                        rg_match = rg_df[rg_df["Virtual Extension Number"] == str(dest_id)]
                        if not rg_match.empty:
                             rg_info = rg_match.iloc[0]
                             for col in rg_info.index:
                                 if col.startswith("User ") and pd.notna(rg_info[col]):
                                      user_name_in_rg = str(rg_info[col])
                                      # Zoek user op naam voor Department info
                                      user_match_by_name = _all_data["users"][_all_data["users"]["Naam"] == user_name_in_rg]
                                      if not user_match_by_name.empty:
                                           user_info = user_match_by_name.iloc[0]
                                           # --- Department Handling --- 
                                           department = user_info.get('Department', 'Geen Afdeling')
                                           if pd.isna(department) or str(department).strip() == "":
                                               department = "Geen Afdeling"
                                           # --- End Department Handling ---
                                           users_found.add((
                                                 user_info.get('Number', 'N/A'),
                                                 user_info.get('Naam', user_name_in_rg),
                                                 str(department) # Zorg dat het een string is
                                           ))
                             noans_dest = rg_info.get("Destination if no answer", np.nan)
                             if pd.notna(noans_dest): queue.append((noans_dest, depth + 1))

                    # Volg DR routes
                    elif dest_type == "DR" or (dest_type == "ExtensionNumber" and not _all_data["receptionists_all"][_all_data["receptionists_all"]["Virtual Extension Number"] == dest_id].empty):
                         rec_df = _all_data.get("receptionists_all", pd.DataFrame())
                         dr_match = rec_df[rec_df["Virtual Extension Number"] == dest_id]
                         if not dr_match.empty:
                              dr_info = dr_match.iloc[0]
                              # Voeg alle mogelijke bestemmingen toe aan de queue
                              dest_cols = ["When office is closed route to", "When on break route to",
                                           "When on holiday route to", "When on holiday route to ", # Spatie variant
                                           "Send call to", "Invalid input destination"]
                              for i in range(10): dest_cols.append(f"Menu {i}")

                              for col in dest_cols:
                                   if col in dr_info.index and pd.notna(dr_info[col]):
                                        queue.append((dr_info[col], depth + 1))

                return users_found

            # Verzamel data per onderdeel
            users_per_onderdeel_data = []
            receptionists_met_onderdeel = receptionists_df_all.dropna(subset=['Onderdeel'])
            alle_onderdelen = receptionists_met_onderdeel['Onderdeel'].unique()

            progress_bar = st.progress(0)
            total_onderdelen = len(alle_onderdelen)

            for i, onderdeel in enumerate(alle_onderdelen):
                drs_in_onderdeel = receptionists_met_onderdeel[receptionists_met_onderdeel['Onderdeel'] == onderdeel]
                reachable_users_in_onderdeel = set()

                for _, dr in drs_in_onderdeel.iterrows():
                    # Start zoektocht vanaf deze DR (directe bestemmingen)
                    dest_cols = ["When office is closed route to", "When on break route to",
                                 "When on holiday route to", "When on holiday route to ",
                                 "Send call to", "Invalid input destination"]
                    for i in range(10): dest_cols.append(f"Menu {i}")

                    for col in dest_cols:
                         if col in dr.index and pd.notna(dr[col]):
                              found_users = find_reachable_users(dr[col], all_data)
                              reachable_users_in_onderdeel.update(found_users)

                # Voeg resultaten toe aan de lijst
                for user_num, user_name, user_dep in reachable_users_in_onderdeel:
                     users_per_onderdeel_data.append({
                         "Onderdeel": onderdeel,
                         "User Number": user_num,
                         "User Name": user_name,
                         "Department": user_dep
                     })
                progress_bar.progress((i + 1) / total_onderdelen)

            if not users_per_onderdeel_data:
                st.info("Geen bereikbare gebruikers gevonden via de Digital Receptionists.")
            else:
                users_per_onderdeel_df = pd.DataFrame(users_per_onderdeel_data)
                # Zorg ervoor dat de kolom Department altijd string is na creatie DataFrame
                users_per_onderdeel_df['Department'] = users_per_onderdeel_df['Department'].astype(str)
                users_per_onderdeel_df = users_per_onderdeel_df.drop_duplicates().sort_values(by=["Onderdeel", "User Name"])

                # Filter opties
                st.subheader("Filter Opties")
                # Nu zouden alle department values strings moeten zijn
                departments = sorted(users_per_onderdeel_df['Department'].unique())
                selected_departments = st.multiselect("Selecteer Afdeling(en):", departments, default=departments)

                if not selected_departments:
                    st.warning("Selecteer minimaal één afdeling.")
                else:
                    filtered_df = users_per_onderdeel_df[users_per_onderdeel_df['Department'].isin(selected_departments)]
                    st.dataframe(filtered_df, use_container_width=True)


    # --- Tab 3: DRs per User ---
    with tab3:
        st.header("Overzicht: Welke DRs/Queues/RGs leiden naar welke User?")

        # Controleer of benodigde dataframes bestaan
        if users_df.empty or receptionists_df_all.empty:
            st.warning("Bestanden 'Users.csv' of 'Receptionists.csv' ontbreken of zijn leeg.")
        elif not (
            'Onderdeel' in receptionists_df_all.columns and
            'Number' in users_df.columns and
            'Department' in users_df.columns and
            'Naam' in users_df.columns
        ):
            st.error("Benodigde kolommen ('Onderdeel', 'Number', 'Department', 'Naam') ontbreken in de CSV-bestanden.")
        else:
            @st.cache_data
            def build_user_reachability_data(_all_data):
                results = []
                users_data = _all_data.get("users", pd.DataFrame())
                receptionists_data = _all_data.get("receptionists_all", pd.DataFrame())
                queues_data = _all_data.get("queues", pd.DataFrame())
                ringgroups_data = _all_data.get("ringgroups", pd.DataFrame())

                # Helper om user info snel op te zoeken (nummer -> details)
                user_dict_by_num = {str(row['Number']): row for _, row in users_data.iterrows()} if 'Number' in users_data.columns else {}
                # Helper om user info snel op te zoeken (naam -> details)
                user_dict_by_name = {str(row['Naam']): row for _, row in users_data.iterrows()} if 'Naam' in users_data.columns else {}

                progress_bar = st.progress(0, text="Analyseren van DR-bestemmingen...")
                total_drs = len(receptionists_data)

                for i, (_, dr) in enumerate(receptionists_data.iterrows()):
                    onderdeel_naam = dr.get('Onderdeel', 'Onbekend Onderdeel')
                    dr_name = dr.get("Digital Receptionist Name", "Naamloos")
                    dr_ext = str(dr.get("Virtual Extension Number", "N/A"))

                    # Verzamel alle bestemmingen van deze DR
                    dest_cols = ["When office is closed route to", "When on break route to",
                                 "When on holiday route to", "When on holiday route to ",
                                 "Send call to", "Invalid input destination"]
                    for menu_idx in range(10): dest_cols.append(f"Menu {menu_idx}")

                    for col in dest_cols:
                        dest_string = dr.get(col, np.nan)
                        if pd.notna(dest_string):
                            dest_type, dest_id = parse_destination(dest_string)

                            # Direct naar User?
                            potential_user_ext = None
                            if dest_type == "User" or dest_type == "ExtensionNumber" or dest_type == "UnknownType":
                                potential_user_ext = str(dest_id) # Gebruik string
                            
                            if potential_user_ext and potential_user_ext in user_dict_by_num:
                                user_info = user_dict_by_num[potential_user_ext]
                                # --- Department Handling --- 
                                department = user_info.get('Department', 'Geen Afdeling')
                                if pd.isna(department) or str(department).strip() == "":
                                    department = "Geen Afdeling"
                                # --- End Department Handling ---
                                results.append({
                                    "User Number": potential_user_ext,
                                    "User Name": user_info.get('Naam', f'User {potential_user_ext}'),
                                    "User Department": str(department), # Zorg voor string
                                    "Reached Via Type": "DR",
                                    "Reached Via Name": dr_name,
                                    "Reached Via Ext": dr_ext,
                                    "Onderdeel": onderdeel_naam
                                })

                            # Naar Queue?
                            elif dest_type == "Queue" or (dest_type == "ExtensionNumber" and not queues_data[queues_data["Virtual Extension Number"] == str(dest_id)].empty):
                                queue_match = queues_data[queues_data["Virtual Extension Number"] == str(dest_id)]
                                if not queue_match.empty:
                                    queue_info = queue_match.iloc[0]
                                    queue_name = queue_info.get('Queue Name', f'Queue {dest_id}')
                                    queue_ext = str(queue_info.get('Virtual Extension Number', dest_id))
                                    for q_col in queue_info.index:
                                        if q_col.startswith("User ") and pd.notna(queue_info[q_col]):
                                            user_name_in_queue = str(queue_info[q_col])
                                            if user_name_in_queue in user_dict_by_name:
                                                user_info = user_dict_by_name[user_name_in_queue]
                                                # --- Department Handling --- 
                                                department = user_info.get('Department', 'Geen Afdeling')
                                                if pd.isna(department) or str(department).strip() == "":
                                                    department = "Geen Afdeling"
                                                # --- End Department Handling ---
                                                results.append({
                                                    "User Number": user_info.get('Number', 'N/A'),
                                                    "User Name": user_name_in_queue,
                                                    "User Department": str(department), # Zorg voor string
                                                    "Reached Via Type": "Queue",
                                                    "Reached Via Name": queue_name,
                                                    "Reached Via Ext": queue_ext,
                                                    "Onderdeel": onderdeel_naam
                                                })

                            # Naar Ring Group?
                            elif dest_type == "RingGroup" or (dest_type == "ExtensionNumber" and not ringgroups_data[ringgroups_data["Virtual Extension Number"] == str(dest_id)].empty):
                                rg_match = ringgroups_data[ringgroups_data["Virtual Extension Number"] == str(dest_id)]
                                if not rg_match.empty:
                                    rg_info = rg_match.iloc[0]
                                    rg_name = rg_info.get('Ring Group Name', f'RG {dest_id}')
                                    rg_ext = str(rg_info.get('Virtual Extension Number', dest_id))
                                    for rg_col in rg_info.index:
                                         if rg_col.startswith("User ") and pd.notna(rg_info[rg_col]):
                                             user_name_in_rg = str(rg_info[rg_col])
                                             if user_name_in_rg in user_dict_by_name:
                                                 user_info = user_dict_by_name[user_name_in_rg]
                                                 # --- Department Handling --- 
                                                 department = user_info.get('Department', 'Geen Afdeling')
                                                 if pd.isna(department) or str(department).strip() == "":
                                                     department = "Geen Afdeling"
                                                 # --- End Department Handling ---
                                                 results.append({
                                                     "User Number": user_info.get('Number', 'N/A'),
                                                     "User Name": user_name_in_rg,
                                                     "User Department": str(department), # Zorg voor string
                                                     "Reached Via Type": "RingGroup",
                                                     "Reached Via Name": rg_name,
                                                     "Reached Via Ext": rg_ext,
                                                     "Onderdeel": onderdeel_naam
                                                 })
                    progress_bar.progress((i + 1) / total_drs, text=f"Analyseren DR {i+1}/{total_drs}...")

                progress_bar.empty() # Verwijder progress bar
                if not results:
                    return pd.DataFrame()

                df = pd.DataFrame(results)
                # Zorg dat User Department altijd string is
                df['User Department'] = df['User Department'].astype(str)
                df = df.drop_duplicates().sort_values(by=["User Name", "Onderdeel", "Reached Via Type", "Reached Via Name"])
                return df

            # Bouw de data en toon de tabel
            drs_per_user_df = build_user_reachability_data(all_data)

            if drs_per_user_df.empty:
                st.info("Geen gebruikers gevonden die bereikt worden via Digital Receptionists, Queues of Ring Groups.")
            else:
                st.subheader("Filter Opties")
                # Filter op User Name
                unique_users = sorted(drs_per_user_df['User Name'].astype(str).unique())
                selected_users = st.multiselect("Filter op Gebruiker:", unique_users, default=[])

                # Filter op Department (nu gegarandeerd strings)
                unique_departments = sorted(drs_per_user_df['User Department'].unique())
                selected_departments = st.multiselect("Filter op Afdeling:", unique_departments, default=[])

                # Filter op Onderdeel
                unique_onderdelen = sorted(drs_per_user_df['Onderdeel'].astype(str).unique())
                selected_onderdelen = st.multiselect("Filter op Onderdeel:", unique_onderdelen, default=[])

                # Filter op Reached Via Type
                unique_types = sorted(drs_per_user_df['Reached Via Type'].astype(str).unique())
                selected_types = st.multiselect("Filter op Bereikt Via Type:", unique_types, default=[])

                # Pas filters toe
                filtered_df = drs_per_user_df.copy()
                if selected_users: filtered_df = filtered_df[filtered_df['User Name'].isin(selected_users)]
                if selected_departments: filtered_df = filtered_df[filtered_df['User Department'].isin(selected_departments)]
                if selected_onderdelen: filtered_df = filtered_df[filtered_df['Onderdeel'].isin(selected_onderdelen)]
                if selected_types: filtered_df = filtered_df[filtered_df['Reached Via Type'].isin(selected_types)]

                st.dataframe(filtered_df, use_container_width=True)

else:
    st.info("Wacht op upload van ZIP-bestand...")