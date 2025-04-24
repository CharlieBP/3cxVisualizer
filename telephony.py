import streamlit as st
import pandas as pd
import graphviz
import os
import numpy as np
import re
import zipfile # Nodig voor ZIP-bestanden
import io      # Nodig om bytes als bestand te behandelen

# Pagina configuratie
st.set_page_config(layout="wide")

# --- Data inladen Functie (Aangepast voor ZIP) ---
# Cache op basis van de *bytes* van het zip bestand voor betere caching
@st.cache_data
def load_data_from_zip(zip_file_bytes):
    """Laadt de benodigde CSV dataframes vanuit de bytes van een ZIP-bestand."""
    data = {}
    required_files = {
        "receptionists": "Receptionists.csv",
        "queues": "Queues.csv",
        "ringgroups": "ringgroups.csv",
        "users": "Users.csv",
        "trunks": "Trunks.csv",
    }
    all_files_found = True
    loaded_files = []
    missing_files = []

    try:
        # Behandel de bytes als een bestand en open het ZIP-archief
        with zipfile.ZipFile(io.BytesIO(zip_file_bytes), 'r') as zf:
            # Controleer of alle vereiste bestanden in het ZIP-archief zitten
            zip_content_list = zf.namelist()
            # Normaliseer paden (verwijder eventuele mappen)
            zip_base_filenames = {os.path.basename(f) for f in zip_content_list}

            for key, filename in required_files.items():
                if filename in zip_base_filenames:
                     # Vind het volledige pad in de zip (kan in een submap zitten)
                     actual_zip_path = next((f for f in zip_content_list if os.path.basename(f) == filename), None)
                     if actual_zip_path:
                         try:
                            # Lees het bestand vanuit het ZIP-archief direct in pandas
                            # Probeer ; dan ,
                            try:
                                df = pd.read_csv(zf.open(actual_zip_path), delimiter=";")
                            except pd.errors.ParserError:
                                # Reset stream positie en probeer met komma
                                with zf.open(actual_zip_path) as file_stream:
                                     df = pd.read_csv(file_stream, delimiter=",")
                            except Exception as parse_err:
                                st.error(f"Fout bij parsen van {filename} in ZIP: {parse_err}")
                                continue # Ga naar volgende bestand

                            data[key] = df
                            loaded_files.append(filename)
                         except Exception as e:
                            st.error(f"Kon {filename} niet correct lezen uit ZIP: {e}")
                            if key in ["receptionists", "queues", "ringgroups", "users"]: all_files_found = False
                     else:
                          # Dit zou niet moeten gebeuren door de check hierboven, maar voor de zekerheid
                          st.warning(f"Kon {filename} niet vinden in ZIP ondanks aanwezigheid in lijst.")
                          missing_files.append(filename)
                          if key in ["receptionists", "queues", "ringgroups", "users"]: all_files_found = False

                else:
                    missing_files.append(filename)
                    st.warning(f"Vereist bestand '{filename}' niet gevonden in het ZIP-archief.")
                    if key in ["receptionists", "queues", "ringgroups", "users"]: all_files_found = False

        if not all_files_found:
            st.error(f"Niet alle essentiële CSV-bestanden ({', '.join(missing_files)}) gevonden in het ZIP-bestand.")
            return None # Geef None terug bij fout

        # --- Data Voorbereiding (direct na laden) ---
        if "receptionists" in data and 'Virtual Extension Number' in data['receptionists'].columns:
            data['receptionists']['Virtual Extension Number'] = data['receptionists']['Virtual Extension Number'].astype(str)
            data["receptionists_primary"] = data["receptionists"][data["receptionists"]["Primair/Secundair"] == "Primair"].copy()
            data["receptionists_all"] = data["receptionists"].copy()
        else:
            data["receptionists_primary"] = pd.DataFrame()
            data["receptionists_all"] = pd.DataFrame()

        if "queues" in data and 'Virtual Extension Number' in data['queues'].columns:
            data['queues']['Virtual Extension Number'] = data['queues']['Virtual Extension Number'].astype(str)
        if "ringgroups" in data and 'Virtual Extension Number' in data['ringgroups'].columns:
            data['ringgroups']['Virtual Extension Number'] = data['ringgroups']['Virtual Extension Number'].astype(str)

        if "users" in data:
            users = data['users']
            if 'Number' in users.columns:
                try:
                    # Forceer naar string, verwijder '.0' indien aanwezig na float conversie
                    users['Number'] = users['Number'].astype(str).str.replace(r'\.0$', '', regex=True)
                except Exception as e:
                     st.warning(f"Kon 'Number' in Users.csv niet opschonen: {e}")
                     users['Number'] = users['Number'].astype(str) # Fallback naar simpele string conversie
            if 'Full Name' in users.columns: users['Naam'] = users['Full Name']
            elif 'Naam' not in users.columns and 'FirstName' in users.columns:
                users['Naam'] = users['FirstName'].fillna('') + ' ' + users['LastName'].fillna('')
                users['Naam'] = users['Naam'].str.strip()
            data['users'] = users

        st.success(f"Succesvol geladen uit ZIP: {', '.join(loaded_files)}")
        return data

    except zipfile.BadZipFile:
        st.error("Ongeldig ZIP-bestand. Upload a.u.b. een correct ZIP-bestand.")
        return None
    except Exception as e:
        st.error(f"Algemene fout bij verwerken ZIP: {e}")
        return None

# --- Helper Functies (parse_destination, format_user_details, get_destination_details blijven ongewijzigd) ---
def parse_destination(dest_string):
    if pd.isna(dest_string) or dest_string == "": return None, None
    dest_string = str(dest_string).strip()
    match_type_id = re.match(r"(\w+)\s?\(\s?(\d+).*", dest_string)
    if match_type_id:
        type_str = match_type_id.group(1).lower(); identifier = match_type_id.group(2)
        if "wachtrij" in type_str or "queue" in type_str: return "Queue", identifier
        if "belgroep" in type_str or "ringgroup" in type_str: return "RingGroup", identifier
        if "gebruiker" in type_str or "user" in type_str or "extension" in type_str: return "User", identifier
        if "digital" in type_str or "receptionist" in type_str or "ivr" in type_str: return "DR", identifier
        if "voicemail" in type_str: return "Voicemail", identifier
        return "UnknownType", identifier
    match_id_name = re.match(r"^(\d{3,})\s+(.*)", dest_string) # Minstens 3 cijfers begin
    if match_id_name:
        identifier = match_id_name.group(1)
        return "ExtensionNumber", identifier
    if dest_string.lower() == "end call": return "EndCall", "End Call"
    if dest_string.lower() == "repeat prompt": return "Repeat", "Repeat Prompt"
    if dest_string.lower() == "accept anyway": return "Accept", "Accept Anyway"
    if dest_string.startswith("+") and dest_string[1:].replace(' ', '').isdigit(): return "External", dest_string
    if dest_string.isdigit(): return "ExtensionNumber", dest_string
    return "UnknownText", dest_string

def format_user_details(user_info):
    details = []
    ext = str(user_info.get('Number', 'N/A')).replace('.0', '') # Verwijder .0
    details.append(f"Ext: {ext}")
    mob = user_info.get('MobileNumber', '');
    if pd.notna(mob) and str(mob).strip(): details.append(f"Mob: {str(mob).strip()}")
    cid = user_info.get('OutboundCallerID', '');
    if pd.notna(cid) and str(cid).strip(): details.append(f"CID: {str(cid).strip()}")
    did_str = user_info.get('DID', ''); first_did = "N/A"
    if pd.notna(did_str) and str(did_str).strip():
        first_did = str(did_str).split(':')[0].strip()
        if first_did: details.append(f"DID: {first_did}")
    return ", ".join(details)

def get_destination_details(identifier, type_hint="Unknown", users_df=None, queues_df=None, ringgroups_df=None, receptionists_df_all=None):
    """Haalt details op, nu met dataframes als argumenten."""
    if users_df is None or queues_df is None or ringgroups_df is None or receptionists_df_all is None:
         return "Fout: DataFrames niet beschikbaar" # Veiligheidscheck

    if pd.isna(identifier) or identifier == "": return "Niet geconfigureerd"
    identifier = str(identifier).strip()

    if type_hint == "EndCall": return "❌ Ophangen"
    if type_hint == "Repeat": return "🔁 Herhaal Prompt"
    if type_hint == "External": return f"📞 Extern:\n{identifier}"
    if type_hint == "Accept": return "➡️ Accepteer / Ga verder"
    if type_hint == "UnknownText": return f"❓ Onbekend:\n{identifier}"

    if identifier.isdigit():
        ext_nr = identifier
        details_found = False
        result_label = f"❓ Onbekende Ext: {ext_nr}"

        # Check Queues
        if not details_found and (type_hint == "Queue" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
            if not queues_df.empty:
                queue = queues_df[queues_df["Virtual Extension Number"] == ext_nr]
                if not queue.empty:
                    queue_info=queue.iloc[0]; queue_name=queue_info.get('Queue Name',f"Queue {ext_nr}")
                    member_details = []
                    for col in queue_info.index:
                        if col.startswith("User ") and pd.notna(queue_info[col]):
                            member_name = queue_info[col]
                            user_detail_row = users_df[users_df['Naam'] == member_name] if not users_df.empty else pd.DataFrame()
                            if not user_detail_row.empty: member_details.append(f"{member_name} ({format_user_details(user_detail_row.iloc[0])})")
                            else: member_details.append(f"{member_name} (❓ Details onbekend)")
                    members_str = "\n ".join(member_details) if member_details else "(Geen leden)"
                    result_label = f"👥 Queue: {queue_name}\n({ext_nr})\nLeden:\n {members_str}"; details_found = True

        # Check Ring Groups
        if not details_found and (type_hint == "RingGroup" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
            if not ringgroups_df.empty:
                rg = ringgroups_df[ringgroups_df["Virtual Extension Number"] == ext_nr]
                if not rg.empty:
                    rg_info=rg.iloc[0]; rg_name=rg_info.get('Ring Group Name',f"Ring Group {ext_nr}")
                    member_details = []
                    for col in rg_info.index:
                         if col.startswith("User ") and pd.notna(rg_info[col]):
                            member_name = rg_info[col]
                            user_detail_row = users_df[users_df['Naam'] == member_name] if not users_df.empty else pd.DataFrame()
                            if not user_detail_row.empty: member_details.append(f"{member_name} ({format_user_details(user_detail_row.iloc[0])})")
                            else: member_details.append(f"{member_name} (❓ Details onbekend)")
                    members_str = "\n ".join(member_details) if member_details else "(Geen leden)"
                    result_label = f"🔔 Ring Group: {rg_name}\n({ext_nr})\nLeden:\n {members_str}"; details_found = True

        # Check Users
        if not details_found and (type_hint == "User" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
            if not users_df.empty:
                user = users_df[users_df["Number"] == ext_nr]
                if not user.empty:
                    user_info = user.iloc[0]; user_name = user_info.get('Naam', f"User {ext_nr}")
                    result_label = f"👤 Gebruiker: {user_name}\n({format_user_details(user_info)})"; details_found = True

        # Check andere DRs
        if not details_found and (type_hint == "DR" or type_hint == "ExtensionNumber" or type_hint == "UnknownType"):
             if not receptionists_df_all.empty:
                 dr = receptionists_df_all[receptionists_df_all["Virtual Extension Number"] == ext_nr]
                 if not dr.empty:
                    dr_info=dr.iloc[0]; dr_name=dr_info.get('Digital Receptionist Name',f"DR {ext_nr}")
                    result_label = f"🚦 IVR: {dr_name}\n({ext_nr})"; details_found = True

        # Check Voicemail
        if not details_found and (type_hint == "Voicemail"):
            user_vm = users_df[users_df["Number"] == ext_nr] if not users_df.empty else pd.DataFrame()
            vm_owner = user_vm.iloc[0].get('Naam', '') if not user_vm.empty else ''
            result_label = f"🎙️ Voicemail\n({ext_nr})\n{'van: '+vm_owner if vm_owner else ''}"; details_found = True

        return result_label

    return f"❓ Onbekend:\n{identifier}"

# --- Streamlit UI Hoofd Sectie ---
st.title("📞 3CX Call Flow Visualizer")
st.markdown("""
Upload een **ZIP-bestand** met daarin de volgende CSV-exports van 3CX:
*   `Receptionists.csv`
*   `Queues.csv`
*   `ringgroups.csv`
*   `Users.csv`
*   `Trunks.csv` (optioneel, momenteel niet direct gebruikt voor grafiek)

*De bestanden mogen in submappen binnen de ZIP staan.*
""")

uploaded_zip = st.file_uploader("Upload CSVs (ZIP)", type="zip", accept_multiple_files=False)

all_data = None # Initialiseer all_data

if uploaded_zip is not None:
    # Lees de inhoud van het geüploade bestand
    zip_content_bytes = uploaded_zip.getvalue()
    # Probeer de data te laden (gecached)
    all_data = load_data_from_zip(zip_content_bytes)

# --- Verwerk en Toon Grafieken (Alleen als data succesvol is geladen) ---
if all_data:
    # Haal de benodigde dataframes op uit de dictionary
    receptionists_df_all = all_data.get("receptionists_all", pd.DataFrame())
    receptionists_df_primary = all_data.get("receptionists_primary", pd.DataFrame())
    queues_df = all_data.get("queues", pd.DataFrame())
    ringgroups_df = all_data.get("ringgroups", pd.DataFrame())
    users_df = all_data.get("users", pd.DataFrame())
    # trunks_df = all_data.get("trunks", pd.DataFrame()) # Nog steeds beschikbaar indien nodig

    st.header("Call Flows per Primaire IVR")
    st.write("Klik op een naam om de flow uit te klappen.")

    if receptionists_df_primary.empty:
        st.warning("Geen primaire Digital Receptionists gevonden in het geüploade 'Receptionists.csv'.")
    else:
        # --- Hoofd Logica: Genereer Grafiek ---
        for _, dr in receptionists_df_primary.iterrows():
            dr_name = dr.get("Digital Receptionist Name", "Naamloos")
            dr_ext = dr.get("Virtual Extension Number", "GEEN_EXT")

            if dr_ext == "GEEN_EXT" or pd.isna(dr_ext):
                st.warning(f"DR '{dr_name}' overgeslagen: geen extensie.")
                continue

            start_label = f"🚦 IVR: {dr_name}\n({dr_ext})"
            dest_strings = {
                'closed': dr.get("When office is closed route to", np.nan),
                'break': dr.get("When on break route to", np.nan),
                'holiday': dr.get(next((col for col in ["When on holiday route to", "When on holiday route to "] if col in dr.index), "non_existing_col"), np.nan),
                'default': dr.get("Send call to", np.nan),
                'invalid': dr.get("Invalid input destination", np.nan)
            }
            menu_options_strings = {}
            has_menu = False
            for i in range(10):
                menu_col = f"Menu {i}"
                if menu_col in dr.index and pd.notna(dr[menu_col]) and str(dr[menu_col]).strip():
                    menu_options_strings[i] = dr[menu_col]; has_menu = True

            with st.expander(f"IVR: {dr_name} ({dr_ext})"):
                dot = graphviz.Digraph(name=f'Flow_{dr_ext}', comment=f'Call Flow for {dr_name}')
                dot.attr(rankdir='LR', size='22,22!', ranksep='0.8', nodesep='0.6', overlap='compress', splines='spline')
                dot.attr('node', shape='box', style='rounded,filled', fillcolor='lightblue', fontname='Arial', fontsize='9')
                dot.attr('edge', fontname='Arial', fontsize='8')

                def make_node_id(prefix, identifier):
                     safe_identifier = re.sub(r'\W+', '_', str(identifier))
                     return f"{prefix}_{dr_ext}_{safe_identifier[:20]}"

                start_node = make_node_id("IVR", dr_ext)
                office_check_node = make_node_id("OFFICECHECK", dr_ext)
                break_check_node = make_node_id("BREAKCHECK", dr_ext)
                holiday_check_node = make_node_id("HOLIDAYCHECK", dr_ext)
                in_hours_node = make_node_id("INHOURS", dr_ext)
                created_nodes = {start_node}

                def create_or_get_node(dot_graph, node_id, label, shape='box', fillcolor='lightblue'):
                     if node_id not in created_nodes:
                         dot_graph.node(node_id, label, shape=shape, fillcolor=fillcolor)
                         created_nodes.add(node_id)
                     return node_id

                # --- Teken de Flow ---
                create_or_get_node(dot, start_node, start_label, shape='Mdiamond', fillcolor='lightcoral')
                create_or_get_node(dot, office_check_node, "Binnen kantooruren?", shape='diamond', fillcolor='lightyellow')
                dot.edge(start_node, office_check_node)

                # Gesloten
                closed_type, closed_id = parse_destination(dest_strings['closed'])
                closed_target_node = make_node_id("END", "closed_fallback") # Default fallback
                closed_target_label = get_destination_details(None, "EndCall", users_df, queues_df, ringgroups_df, receptionists_df_all)
                if closed_type:
                    closed_target_node = make_node_id(f"DEST_{closed_type}", f"closed_{closed_id}")
                    closed_target_label = get_destination_details(closed_id, closed_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                create_or_get_node(dot, closed_target_node, closed_target_label)
                dot.edge(office_check_node, closed_target_node, label="Nee")

                # Pauze Check
                create_or_get_node(dot, break_check_node, "Pauze actief?", shape='diamond', fillcolor='lightyellow')
                dot.edge(office_check_node, break_check_node, label="Ja")

                # Pauze Route
                break_type, break_id = parse_destination(dest_strings['break'])
                break_target_node = make_node_id("END", "break_fallback") # Default fallback
                break_target_label = get_destination_details(None, "EndCall", users_df, queues_df, ringgroups_df, receptionists_df_all)
                if break_type:
                    break_target_node = make_node_id(f"DEST_{break_type}", f"break_{break_id}")
                    break_target_label = get_destination_details(break_id, break_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                create_or_get_node(dot, break_target_node, break_target_label)
                dot.edge(break_check_node, break_target_node, label="Ja")

                # Vakantie Check
                create_or_get_node(dot, holiday_check_node, "Vakantie actief?", shape='diamond', fillcolor='lightyellow')
                dot.edge(break_check_node, holiday_check_node, label="Nee")

                # Vakantie Route
                holiday_type, holiday_id = parse_destination(dest_strings['holiday'])
                holiday_target_node = in_hours_node # Default: ga verder naar in_hours
                holiday_edge_label = "Ja (geen route)"
                if holiday_type:
                     holiday_target_node = make_node_id(f"DEST_{holiday_type}", f"holiday_{holiday_id}")
                     holiday_target_label = get_destination_details(holiday_id, holiday_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                     create_or_get_node(dot, holiday_target_node, holiday_target_label)
                     holiday_edge_label="Ja"
                dot.edge(holiday_check_node, holiday_target_node, label=holiday_edge_label)


                # Geen Vakantie -> In Hours Actie Node
                create_or_get_node(dot, in_hours_node, "Actie binnen kantooruren", shape='ellipse', fillcolor='lightgrey')
                dot.edge(holiday_check_node, in_hours_node, label="Nee")

                # Menu / Directe Route
                if has_menu:
                    dot.node(in_hours_node, "🎶 Menu speelt...")
                    # Menu opties
                    for key, dest_str in menu_options_strings.items():
                        menu_type, menu_id = parse_destination(dest_str)
                        if menu_type:
                            menu_label = get_destination_details(menu_id, menu_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                            menu_node_id = make_node_id(f"MENU{key}_{menu_type}", menu_id)
                            if menu_type == "Repeat":
                                repeat_node_id = make_node_id("REPEAT", f"menu{key}")
                                create_or_get_node(dot, repeat_node_id, menu_label, shape='invhouse', fillcolor='orange')
                                dot.edge(in_hours_node, repeat_node_id, label=f"Kies {key}")
                                dot.edge(repeat_node_id, in_hours_node, label="Herhaal")
                            elif menu_type == "Accept":
                                accept_node_id = make_node_id("ACCEPT", f"menu{key}")
                                create_or_get_node(dot, accept_node_id, menu_label, fillcolor='lightgreen')
                                dot.edge(in_hours_node, accept_node_id, label=f"Kies {key}")
                                # Link naar timeout/default target
                                default_type_acc, default_id_acc = parse_destination(dest_strings['default'])
                                acc_target_node = make_node_id("END", "accept_fallback")
                                acc_target_label = get_destination_details(None, "EndCall", users_df, queues_df, ringgroups_df, receptionists_df_all)
                                if default_type_acc:
                                    acc_target_node = make_node_id(f"DEST_{default_type_acc}", f"timeout_{default_id_acc}")
                                    acc_target_label = get_destination_details(default_id_acc, default_type_acc, users_df, queues_df, ringgroups_df, receptionists_df_all)
                                create_or_get_node(dot, acc_target_node, acc_target_label)
                                dot.edge(accept_node_id, acc_target_node, label="Verder")
                            else: # Normale bestemming
                                create_or_get_node(dot, menu_node_id, menu_label)
                                dot.edge(in_hours_node, menu_node_id, label=f"Kies {key}")

                    # Timeout / Default Actie
                    default_type, default_id = parse_destination(dest_strings['default'])
                    timeout_target_node = make_node_id("END", "timeout_fallback")
                    timeout_target_label = get_destination_details(None, "EndCall", users_df, queues_df, ringgroups_df, receptionists_df_all)
                    timeout_edge_label = "Timeout /\nGeen invoer"
                    if default_type:
                        timeout_target_node = make_node_id(f"DEST_{default_type}", f"timeout_{default_id}")
                        timeout_target_label = get_destination_details(default_id, default_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                        if default_type == "Repeat": # Check voor Repeat bij timeout
                            timeout_target_node = make_node_id("REPEAT", "timeout") # Aparte node ID
                            create_or_get_node(dot, timeout_target_node, timeout_target_label, shape='invhouse', fillcolor='orange')
                            dot.edge(in_hours_node, timeout_target_node, label=timeout_edge_label)
                            dot.edge(timeout_target_node, in_hours_node, label="Herhaal") # Pijl terug
                            continue # Sla normale edge creatie over voor repeat
                    create_or_get_node(dot, timeout_target_node, timeout_target_label)
                    dot.edge(in_hours_node, timeout_target_node, label=timeout_edge_label)


                    # Invalid Input Actie
                    invalid_type, invalid_id = parse_destination(dest_strings['invalid'])
                    is_different = not (invalid_type == default_type and invalid_id == default_id)
                    if invalid_type and is_different:
                        invalid_target_node = make_node_id(f"DEST_{invalid_type}", f"invalid_{invalid_id}")
                        invalid_target_label = get_destination_details(invalid_id, invalid_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                        if invalid_type == "Repeat": # Check voor Repeat bij invalid
                             invalid_target_node = make_node_id("REPEAT", "invalid") # Aparte node ID
                             create_or_get_node(dot, invalid_target_node, invalid_target_label, shape='invhouse', fillcolor='orange')
                             dot.edge(in_hours_node, invalid_target_node, label="Ongeldige invoer")
                             dot.edge(invalid_target_node, in_hours_node, label="Herhaal") # Pijl terug
                        else:
                             create_or_get_node(dot, invalid_target_node, invalid_target_label)
                             dot.edge(in_hours_node, invalid_target_node, label="Ongeldige invoer")

                else: # Geen Menu
                    dot.node(in_hours_node, "Geen menu", shape='ellipse', fillcolor='lightgrey')
                    direct_type, direct_id = parse_destination(dest_strings['default'])
                    direct_target_node = make_node_id("END", "direct_fallback")
                    direct_target_label = get_destination_details(None, "EndCall", users_df, queues_df, ringgroups_df, receptionists_df_all)
                    if direct_type:
                         direct_target_node = make_node_id(f"DEST_{direct_type}", f"direct_{direct_id}")
                         direct_target_label = get_destination_details(direct_id, direct_type, users_df, queues_df, ringgroups_df, receptionists_df_all)
                    create_or_get_node(dot, direct_target_node, direct_target_label)
                    dot.edge(in_hours_node, direct_target_node, label="Directe route")

                # Toon de grafiek
                try:
                    st.graphviz_chart(dot, use_container_width=True)
                except Exception as e_graph:
                     st.error(f"Fout bij genereren grafiek voor {dr_name} ({dr_ext}): {e_graph}")
                     # st.code(dot.source, language='dot') # Optioneel: toon DOT source bij fout

elif uploaded_zip is None:
    st.info("Wacht op upload van ZIP-bestand...")