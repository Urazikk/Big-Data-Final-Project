import time
import json
import random
from kafka import KafkaProducer

# --- 1. CONFIGURATION KAFKA ---
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
TOPIC_NAME = 'sports-events'

# --- 2. CONFIGURATION DES MATCHS ---
ACTIVE_MATCHES = [
    {"id": "LIGUE1_01", "home": "PSG", "away": "OM", "league": "Ligue 1"},
    {"id": "LIGUE1_02", "home": "Lyon", "away": "Lens", "league": "Ligue 1"},
    {"id": "PREM_01", "home": "Man City", "away": "Liverpool", "league": "Premier League"},
    {"id": "LIGA_01", "home": "Real Madrid", "away": "Barca", "league": "La Liga"},
    {"id": "BUNDES_01", "home": "Bayern", "away": "Dortmund", "league": "Bundesliga"}
]

# --- 3. EFFECTIFS PAR POSTE (11 JOUEURS EXACTS) ---
TEAMS_STRUCTURED = {
    "PSG": {
        "GK": ["Donnarumma"], 
        "DEF": ["Hakimi", "Marquinhos", "Pacho", "Mendes"],
        "MID": ["Zaïre-Emery", "Vitinha", "Neves"], 
        "FWD": ["Dembélé", "Ramos", "Barcola"]
    },
    "OM": {
        "GK": ["Rulli"], 
        "DEF": ["Balerdi", "Brassier", "Murillo", "Merlin"],
        "MID": ["Højbjerg", "Rabiot", "Kondogbia"], 
        "FWD": ["Greenwood", "Wahi", "Henrique"]
    },
    "Lyon": {
        "GK": ["Perri"], 
        "DEF": ["Tagliafico", "Niakhaté", "Caleta-Car", "Maitland-Niles"],
        "MID": ["Veretout", "Tolisso", "Caqueret"], 
        "FWD": ["Lacazette", "Cherki", "Fofana"]
    },
    "Lens": {
        "GK": ["Samba"], 
        "DEF": ["Gradit", "Danso", "Medina"],
        "MID": ["Thomasson", "Diouf", "Frankowski", "Machado"], 
        "FWD": ["Sotoca", "Nzola", "Said"]
    },
    "Man City": {
        "GK": ["Ederson"], 
        "DEF": ["Walker", "Dias", "Akanji", "Gvardiol"],
        "MID": ["Rodri", "De Bruyne", "Bernardo", "Foden"], 
        "FWD": ["Haaland", "Doku"]
    },
    "Liverpool": {
        "GK": ["Alisson"], 
        "DEF": ["Arnold", "Van Dijk", "Konaté", "Robertson"],
        "MID": ["Mac Allister", "Szoboszlai", "Gravenberch"], 
        "FWD": ["Salah", "Nunez", "Diaz"]
    },
    "Real Madrid": {
        "GK": ["Courtois"], 
        "DEF": ["Carvajal", "Militao", "Rudiger", "Mendy"],
        "MID": ["Valverde", "Tchouameni", "Bellingham"], 
        "FWD": ["Mbappé", "Vinicius Jr", "Rodrygo"]
    },
    "Barca": {
        "GK": ["Ter Stegen"], 
        "DEF": ["Koundé", "Cubarsi", "Martinez", "Balde"],
        "MID": ["Casado", "Pedri", "Olmo"], 
        "FWD": ["Yamal", "Lewandowski", "Raphinha"]
    },
    "Bayern": {
        "GK": ["Neuer"], 
        "DEF": ["Upamecano", "Kim", "Davies", "Guerreiro"],
        "MID": ["Kimmich", "Palhinha", "Musiala"], 
        "FWD": ["Kane", "Sané", "Olise"]
    },
    "Dortmund": {
        "GK": ["Kobel"], 
        "DEF": ["Anton", "Schlotterbeck", "Ryerson", "Bensebaini"],
        "MID": ["Can", "Gross", "Brandt"], 
        "FWD": ["Guirassy", "Malen", "Adeyemi"]
    }
}

# --- 4. MATRICE DE PROBABILITÉS ---
POSSESSION_WEIGHTS = {"GK": 5, "DEF": 30, "MID": 45, "FWD": 20}
ACTION_TYPES = ["PASS", "SHOT", "FOUL", "CORNER", "GOAL", "CARD_YELLOW"]


ACTION_WEIGHTS_BY_POS = {
    # Gardien
    "GK":  [99,  0,   0.5, 0,    0,    0.1], 
        
    "DEF": [94,  0.75, 3,   0.5,  0.02, 0.4], 
    

    "MID": [88,  3,   4,   1,    0.2,  0.3], 
    
    "FWD": [70,  10,  3,   2,    2.4,  0.2] 
}

# --- 5. FONCTION XG ---
def calculate_xg(event_type, role, x_pos):
    if event_type == 'GOAL': return 1.0
    if event_type == 'SHOT':
        distance_factor = (x_pos / 100) ** 2
        base_xg = 0.05
        if role == 'FWD': base_xg = 0.12 # Légère baisse
        elif role == 'MID': base_xg = 0.06
        shot_quality = random.uniform(0.5, 1.5)
        return round(min(base_xg * distance_factor * shot_quality, 0.99), 2)
    return 0.0

# --- 6. STATS TRACKING ---

match_stats_tracker = {m['id']: {'players': {}, 'banned': []} for m in ACTIVE_MATCHES}

def update_stats(match_id, player, event_type, x_pos):
    match_data = match_stats_tracker[match_id]
    
    if player not in match_data['players']:
        match_data['players'][player] = {
            'goals': 0, 'shots': 0, 'passes': 0, 
            'x_pos_total': 0, 'actions': 0, 'yellow_cards': 0
        }
    
    stats = match_data['players'][player]
    stats['actions'] += 1
    stats['x_pos_total'] += x_pos
    
    if event_type == 'GOAL': stats['goals'] += 1
    elif event_type == 'SHOT': stats['shots'] += 1
    elif event_type == 'PASS': stats['passes'] += 1
    elif event_type == 'CARD_YELLOW': stats['yellow_cards'] += 1

# --- 7. GESTION DU TEMPS ---
match_time = 0.0 
half_time_passed = False 

print(f"🏟️  DÉBUT DU MULTIPLEX (Réglage : ÉQUILIBRÉ & CARTONS ROUGES)")
print(f"📡  Envoi vers Kafka ('{TOPIC_NAME}')")
print("----------------------------------------------------------------")

try:
    end_time = 90 + random.randint(2, 5)
    
    while match_time <= end_time:
        if match_time > 45 and not half_time_passed:
            print("\n⏸️  C'EST LA MI-TEMPS ! (Pause courte...)")
            time.sleep(1.0)
            print("▶️  REPRISE DES MATCHS (Seconde période)\n")
            half_time_passed = True
            
        match = random.choice(ACTIVE_MATCHES)
        match_id = match['id']
        team_name = match['home'] if random.random() > 0.5 else match['away']
        opponent_name = match['away'] if team_name == match['home'] else match['home']
        
        positions = list(POSSESSION_WEIGHTS.keys())
        weights = list(POSSESSION_WEIGHTS.values())
        selected_pos = random.choices(positions, weights=weights, k=1)[0]
        
        if team_name in TEAMS_STRUCTURED:
            player_name = random.choice(TEAMS_STRUCTURED[team_name][selected_pos])
        else:
            player_name = "Unknown Player"

        # VÉRIFICATION CARTON ROUGE (Le joueur joue-t-il encore ?)
        if player_name in match_stats_tracker[match_id]['banned']:
            # L'équipe joue à 10, donc on saute ce tour (moins d'actions pour cette équipe)
            # On avance juste un peu le temps pour simuler le jeu qui continue
            match_time += 0.01 
            continue

        actions = ACTION_TYPES
        action_weights = ACTION_WEIGHTS_BY_POS[selected_pos]
        event_type = random.choices(actions, weights=action_weights, k=1)[0]
        
        # LOGIQUE CARTON ROUGE AUTOMATIQUE (2 Jaunes = Rouge)
        if event_type == 'CARD_YELLOW':
         
            current_yellows = 0
            if player_name in match_stats_tracker[match_id]['players']:
                current_yellows = match_stats_tracker[match_id]['players'][player_name]['yellow_cards']
            
            if current_yellows >= 1:
                event_type = 'CARD_RED'
                match_stats_tracker[match_id]['banned'].append(player_name)
                

        x_coord = round(random.uniform(50, 98) if event_type in ['SHOT', 'GOAL'] else random.uniform(0, 100), 1)
        xg_value = calculate_xg(event_type, selected_pos, x_coord)
        
        # Mise à jour des stats
        update_stats(match_id, player_name, event_type, x_coord)

        current_minute = int(match_time)
        event = {
            'match_id': match['id'],
            'league': match['league'],
            'home_team': match['home'],
            'away_team': match['away'],
            'timestamp': int(time.time()),
            'game_time': current_minute,
            'event_type': event_type,
            'team': team_name,
            'opponent': opponent_name,
            'player': player_name,
            'position_role': selected_pos,
            'coordinate_x': x_coord,
            'xg': xg_value
        }

        producer.send(TOPIC_NAME, value=event)

        # Logs
        time_display = f"[{current_minute}'][{match['league']}]"
        
        if event_type == 'GOAL':
            print(f"{time_display} ⚽ BUUUUT !!! {player_name} ({team_name})")
        elif event_type == 'SHOT':
            print(f"{time_display} 🚀 Tir de {player_name} ({team_name})")
        elif event_type == 'CARD_YELLOW':
            print(f"{time_display} 🟨 Jaune pour {player_name}")
        elif event_type == 'CARD_RED':
             print(f"{time_display} 🟥 ROUGE pour {player_name} (2ème Jaune) !!!")
        else:
            if random.random() < 0.05: 
                print(f"{time_display} {event_type} - {team_name}")

        match_time += random.uniform(0.02, 0.06) 
        time.sleep(random.uniform(0.01, 0.05))

    print("\n🏁  FIN DU TEMPS RÉGLEMENTAIRE")
    print("----------------------------------------------------------------")
    print("🏆  RÉSULTATS & HOMMES DU MATCH")
    print("----------------------------------------------------------------")
    
    for match in ACTIVE_MATCHES:
        mid = match['id']
        home = match['home']
        away = match['away']
        
        best_player = None
        max_score = -1
        
        match_data = match_stats_tracker.get(mid, {})
        players_data = match_data.get('players', {})
        
        if not players_data:
            continue
            
        for player, stats in players_data.items():
            # Score pondéré : But=50, Tir=10, Passe=1
            score = (stats['goals'] * 50) + (stats['shots'] * 10) + (stats['passes'] * 1)
            # Malus pour carton rouge pour éviter qu'un exclu soit homme du match
            if player in match_data['banned']:
                score -= 100
                
            if score > max_score:
                max_score = score
                best_player = player
        
        if best_player:
            bp_stats = players_data[best_player]
            print(f"\n⚽ MATCH : {home} vs {away}")
            print(f"⭐ HOMME DU MATCH : {best_player}")
            print(f"   📊 Stats   : {bp_stats['goals']} Buts, {bp_stats['shots']} Tirs, {bp_stats['passes']} Passes")

    print("\n⏳  Le script reste ouvert 20 secondes pour laisser Spark finir ses calculs...")
    time.sleep(20)
    
    print("✅  Arrêt du producteur.")
    producer.close()

except KeyboardInterrupt:
    print("\n🛑 Arrêt manuel.")
    producer.close()