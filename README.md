# 3CX Call Flow Visualizer

Deze Streamlit-applicatie visualiseert 3CX telefooncentrale configuraties op basis van geëxporteerde CSV-bestanden.

## Functionaliteit

De applicatie leest een ZIP-bestand met daarin de volgende 3CX CSV-exportbestanden:

*   `Receptionists.csv`: Bevat informatie over Digital Receptionists (IVR's).
*   `Queues.csv`: Bevat informatie over wachtrijen.
*   `ringgroups.csv`: Bevat informatie over belgroepen.
*   `Users.csv`: Bevat informatie over gebruikers/extensies.
*   `Trunks.csv` (optioneel, wordt geladen maar momenteel niet actief gebruikt in visualisaties).

Op basis van deze bestanden genereert de applicatie de volgende overzichten in aparte tabbladen:

1.  **Flows per Onderdeel / Individuele DR:**
    *   Visualiseert de belstroom (call flow) voor elke Digital Receptionist (IVR).
    *   DR's worden gegroepeerd op basis van de kolom `Onderdeel` in `Receptionists.csv`. Voor elk uniek onderdeel wordt een gecombineerde flow getoond die start bij het onderdeel en linkt naar de bijbehorende (primaire) DR(s).
    *   DR's waarvoor de kolom `Onderdeel` leeg, `NaN`, of `?` is, worden apart behandeld en krijgen elk hun eigen individuele flow-diagram.
    *   De flows tonen menu-opties, tijdscondities (kantooruren, pauze, vakantie), en de uiteindelijke bestemmingen (andere DRs, wachtrijen, belgroepen, gebruikers, voicemail, externe nummers, ophangen).
2.  **Users per Onderdeel:**
    *   Toont een tabel met alle gebruikers die bereikt kunnen worden via de flows die starten bij de DRs binnen een specifiek `Onderdeel`.
    *   Dit omvat gebruikers die direct worden bereikt, of indirect via wachtrijen of belgroepen.
    *   De tabel bevat de naam, het extensienummer en de afdeling van de gebruiker, en het onderdeel waartoe de initiële DR behoort.
    *   De tabel is filterbaar op afdeling.
3.  **DRs per User:**
    *   Toont een tabel die laat zien via welke Digital Receptionist, Wachtrij of Belgroep een specifieke gebruiker bereikt kan worden.
    *   De tabel bevat de naam, het extensienummer en de afdeling van de gebruiker, het type bestemming (DR/Queue/RingGroup) waardoor de gebruiker bereikt wordt, de naam en extensie van die bestemming, en het `Onderdeel` van de DR die initieel naar deze bestemming leidt.
    *   De tabel is filterbaar op gebruiker, afdeling, onderdeel en type bestemming.

## Setup

1.  **Python:** Zorg dat Python 3 (bij voorkeur 3.9+) geïnstalleerd is.
2.  **Dependencies:** Installeer de benodigde Python packages:
    ```bash
    pip install -r requirements.txt
    ```
    De `requirements.txt` bevat:
    *   `streamlit`
    *   `pandas`
    *   `graphviz`
3.  **Graphviz Systeem Installatie:** Streamlit's `graphviz_chart` vereist dat Graphviz op je systeem geïnstalleerd is. Volg de instructies op de [officiële Graphviz downloadpagina](https://graphviz.org/download/) voor jouw besturingssysteem.
    *   **Windows:** Download het installatieprogramma en voeg de Graphviz `bin` map toe aan je systeem PATH environment variable.
    *   **macOS (met Homebrew):** `brew install graphviz`
    *   **Linux (Debian/Ubuntu):** `sudo apt update && sudo apt install graphviz -y`
    *   **Linux (Fedora):** `sudo dnf install graphviz`

## Uitvoeren

1.  Navigeer in je terminal naar de map waar `telephony.py` staat.
2.  Start de Streamlit applicatie:
    ```bash
    streamlit run telephony.py
    ```
3.  De applicatie opent automatisch in je webbrowser.
4.  Upload het ZIP-bestand met de benodigde CSV-bestanden via de file uploader in de applicatie.
5.  Bekijk de gegenereerde flows en overzichten in de verschillende tabbladen.

## Benodigde CSV Kolommen

Voor een correcte werking zijn specifieke kolomnamen essentieel in de CSV-bestanden:

*   **Receptionists.csv:** `Onderdeel`, `Primair/Secundair`, `Digital Receptionist Name`, `Virtual Extension Number`, `Menu 0` t/m `Menu 9`, `When office is closed route to`, `When on break route to`, `When on holiday route to`, `Send call to`, `Invalid input destination`, `If no input within seconds`.
*   **Users.csv:** `Number`, `FirstName`, `LastName`, `Naam` (of `Full Name`), `Department`.
*   **Queues.csv:** `Virtual Extension Number`, `Queue Name`, `Ring time (s)`, `Max queue wait time (s)`, `Destination if no answer`, `User 1`, `User 2`, etc.
*   **ringgroups.csv:** `Virtual Extension Number`, `Ring Group Name`, `Ring time (s)`, `Destination if no answer`, `User 1`, `User 2`, etc.

*Let op:* De applicatie probeert flexibel te zijn met kolomnamen (bijv. `Naam` vs `Full Name`), maar de aanwezigheid van de kernkolommen is cruciaal. 