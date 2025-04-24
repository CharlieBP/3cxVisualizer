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
st.title("📞 3CX Call Flow Visualizer (Per IVR)")
st.markdown("Upload een **ZIP-bestand** met `Receptionists.csv`, `Queues.csv`, `ringgroups.csv`, `Users.csv`.")

uploaded_zip = st.file_uploader("Upload CSVs (ZIP)", type="zip")
all_data = None

if uploaded_zip is not None:
    zip_content_bytes = uploaded_zip.getvalue()
    all_data = load_data_from_zip(zip_content_bytes)

if all_data:
    receptionists_df_primary = all_data.get("receptionists_primary", pd.DataFrame())
    # Haal deze hier op zodat ze beschikbaar zijn in de loop
    queues_df = all_data.get("queues", pd.DataFrame())
    ringgroups_df = all_data.get("ringgroups", pd.DataFrame())

    st.header("Call Flows per Primaire IVR")
    st.write("Klik op een naam om de flow uit te klappen.")

    if receptionists_df_primary.empty:
        st.warning("Geen primaire Digital Receptionists gevonden.")
    else:
        for _, dr in receptionists_df_primary.iterrows():
            dr_name = dr.get("Digital Receptionist Name", "Naamloos")
            dr_ext = dr.get("Virtual Extension Number", "GEEN_EXT")
            if dr_ext == "GEEN_EXT" or pd.isna(dr_ext): continue
            ivr_timeout_sec_val = dr.get("If no input within seconds", None)
            ivr_timeout_label = f" ({ivr_timeout_sec_val}s)" if pd.notna(ivr_timeout_sec_val) else ""

            with st.expander(f"IVR: {dr_name} ({dr_ext})"):
                dot = graphviz.Digraph(name=f'Flow_{dr_ext}', comment=f'Call Flow for {dr_name}')
                dot.attr(rankdir='LR', size='25,25!', ranksep='0.8', nodesep='0.6', overlap='prism', splines='spline')
                dot.attr('node', shape='box', style='rounded,filled', fontname='Arial', fontsize='9')
                dot.attr('edge', fontname='Arial', fontsize='8')
                added_nodes = set(); added_edges = set()

                def make_node_id(prefix, identifier):
                    safe_identifier = re.sub(r'\W+', '_', str(identifier))
                    return f"{prefix}_{dr_ext}_{safe_identifier[:20]}"

                def create_or_get_node(dot_graph, node_id, label, shape='box', fillcolor='lightblue'):
                     if node_id not in added_nodes:
                         dot_graph.node(node_id, label, shape=shape, fillcolor=fillcolor)
                         added_nodes.add(node_id)
                     return node_id

                # *** Helper krijgt nu all_data mee ***
                def draw_destination(source_node_id, edge_label, dest_string, current_all_data):
                    dest_type, dest_id = parse_destination(dest_string)
                    target_node_id = make_node_id("END", f"{source_node_id}_{edge_label.replace(' ','_')}")
                    # *** Geef all_data door ***
                    target_label, target_shape, target_color, target_node_type = get_node_label_and_style(None, "EndCall", current_all_data)
                    if dest_type:
                        target_node_id = make_node_id(f"DEST_{dest_type}", f"{source_node_id}_{edge_label.replace(' ','_')}_{dest_id}")
                        # *** Geef all_data door ***
                        target_label, target_shape, target_color, target_node_type = get_node_label_and_style(dest_id, dest_type, current_all_data)

                    create_or_get_node(dot, target_node_id, target_label, shape=target_shape, fillcolor=target_color)
                    edge_key = (source_node_id, target_node_id, edge_label)
                    if edge_key not in added_edges: dot.edge(source_node_id, target_node_id, label=edge_label); added_edges.add(edge_key)

                    # Teken 'No Answer' voor Queue/RG
                    if target_node_type in ["Queue", "RingGroup"] and dest_id:
                         q_df = current_all_data.get("queues", pd.DataFrame())
                         rg_df = current_all_data.get("ringgroups", pd.DataFrame())
                         df_lookup = q_df if target_node_type == "Queue" else rg_df
                         row = df_lookup[df_lookup["Virtual Extension Number"] == str(dest_id)]
                         if not row.empty:
                             noans_dest = row.iloc[0].get("Destination if no answer", np.nan)
                             noans_type, noans_id = parse_destination(noans_dest)
                             noans_target_node_id = make_node_id("END", f"{target_node_id}_NoAns")
                             # *** Geef all_data door ***
                             noans_label, noans_shape, noans_color, _ = get_node_label_and_style(None, "EndCall", current_all_data)
                             if noans_type:
                                  noans_target_node_id = make_node_id(f"NOANS_{noans_type}", f"{target_node_id}_{noans_id}")
                                  # *** Geef all_data door ***
                                  noans_label, noans_shape, noans_color, _ = get_node_label_and_style(noans_id, noans_type, current_all_data)
                             create_or_get_node(dot, noans_target_node_id, noans_label, shape=noans_shape, fillcolor=noans_color)
                             noans_edge_key = (target_node_id, noans_target_node_id, "No Answer")
                             if noans_edge_key not in added_edges: dot.edge(target_node_id, noans_target_node_id, label="No Answer"); added_edges.add(noans_edge_key)

                # --- Teken de flow ---
                start_node_id = make_node_id("IVR", dr_ext)
                # *** Geef all_data door ***
                start_label, start_shape, start_color, _ = get_node_label_and_style(dr_ext, "DR", all_data)
                create_or_get_node(dot, start_node_id, start_label, shape=start_shape, fillcolor=start_color)

                # Tijd checks
                office_check_node = make_node_id("OFFICECHECK", "")
                # *** Geef all_data door (ook al niet direct nodig) ***
                _, office_shape, office_color, _ = get_node_label_and_style("","Check", all_data)
                create_or_get_node(dot, office_check_node, "Binnen kantooruren?", shape=office_shape, fillcolor=office_color); dot.edge(start_node_id, office_check_node)
                break_check_node = make_node_id("BREAKCHECK", ""); create_or_get_node(dot, break_check_node, "Pauze actief?", shape=office_shape, fillcolor=office_color); dot.edge(office_check_node, break_check_node, label="Ja")
                holiday_check_node = make_node_id("HOLIDAYCHECK", ""); create_or_get_node(dot, holiday_check_node, "Vakantie actief?", shape=office_shape, fillcolor=office_color); dot.edge(break_check_node, holiday_check_node, label="Nee")
                in_hours_node = make_node_id("INHOURS", ""); create_or_get_node(dot, in_hours_node, "Actie binnen kantooruren", shape='ellipse', fillcolor='lightgrey'); dot.edge(holiday_check_node, in_hours_node, label="Nee")

                # Haal bestemmingen op
                dest_strings = { 'closed': dr.get("When office is closed route to", np.nan), 'break': dr.get("When on break route to", np.nan), 'holiday': dr.get(next((col for col in ["When on holiday route to", "When on holiday route to "] if col in dr.index), "non_existing_col"), np.nan), 'default': dr.get("Send call to", np.nan), 'invalid': dr.get("Invalid input destination", np.nan) }
                menu_options_strings = {}; has_menu = False
                for i in range(10):
                    menu_col = f"Menu {i}";
                    if menu_col in dr.index and pd.notna(dr[menu_col]) and str(dr[menu_col]).strip(): menu_options_strings[i] = dr[menu_col]; has_menu = True

                # Teken tijd-gebaseerde routes
                # *** Geef all_data door ***
                draw_destination(office_check_node, "Nee", dest_strings['closed'], all_data)
                draw_destination(break_check_node, "Ja", dest_strings['break'], all_data)
                # Vakantie route
                holiday_type, holiday_id = parse_destination(dest_strings['holiday'])
                if holiday_type:
                    # *** Geef all_data door ***
                    draw_destination(holiday_check_node, "Ja", dest_strings['holiday'], all_data)
                else:
                     edge_key = (holiday_check_node, in_hours_node, "Ja (geen route)")
                     if edge_key not in added_edges: dot.edge(holiday_check_node, in_hours_node, label="Ja (geen route)"); added_edges.add(edge_key)

                # Menu / Directe Route
                if has_menu:
                    dot.node(in_hours_node, "🎶 Menu speelt...")
                    for key, dest_str in menu_options_strings.items():
                         # *** Geef all_data door ***
                        draw_destination(in_hours_node, f"Kies {key}", dest_str, all_data)

                    # Timeout pijl met tijd
                    timeout_edge_label = f"Timeout{ivr_timeout_label} /\nGeen invoer"
                    # *** Geef all_data door ***
                    draw_destination(in_hours_node, timeout_edge_label, dest_strings['default'], all_data)

                    # Invalid (als anders)
                    if dest_strings['invalid'] != dest_strings['default'] and pd.notna(dest_strings['invalid']):
                        # *** Geef all_data door ***
                        draw_destination(in_hours_node, "Invalid", dest_strings['invalid'], all_data)
                else: # Geen menu
                    dot.node(in_hours_node, "Geen menu")
                    # *** Geef all_data door ***
                    draw_destination(in_hours_node, "Direct", dest_strings['default'], all_data)

                # Toon grafiek
                try: st.graphviz_chart(dot, use_container_width=True)
                except Exception as e: st.error(f"Fout genereren grafiek: {e}"); st.code(dot.source, language='dot')

else:
    st.info("Wacht op upload van ZIP-bestand...")