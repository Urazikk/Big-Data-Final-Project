import time
import json
import random
from kafka import KafkaProducer

# --- 1. KAFKA CONFIGURATION ---
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)
TOPIC_NAME = 'sports-events'

# --- 2. MATCH CONFIGURATION ---
ACTIVE_MATCHES = [
    {"id": "LIGUE1_01", "home": "PSG", "away": "OM", "league": "Ligue 1"},
    {"id": "LIGUE1_02", "home": "Lyon", "away": "ASM", "league": "Ligue 1"},
    {"id": "PREM_01", "home": "Man City", "away": "Liverpool", "league": "Premier League"},
    {"id": "LIGA_01", "home": "Real Madrid", "away": "Barca", "league": "La Liga"},
    {"id": "BUNDES_01", "home": "Bayern", "away": "Dortmund", "league": "Bundesliga"}
]

# --- 3. ROSTERS (11 PLAYERS) ---
TEAMS_STRUCTURED = {
    "PSG": {
        "GK": ["Chevalier"], 
        "DEF": ["Hakimi", "Marquinhos", "Pacho", "Mendes"],
        "MID": ["Fabian", "Vitinha", "Neves"], 
        "FWD": ["Dembélé", "Doué", "Barcola"] 
    },
    "OM": {
        "GK": ["Rulli"], 
        "DEF": ["Pavard", "Aguerd", "Balerdi", "Emerson"],
        "MID": ["Højbjerg", "Vermeeren", "Kondogbia"], 
        "FWD": ["Greenwood", "Aubameyang", "Paixao"] 
    },
    "Lyon": {
        "GK": ["Perri"], 
        "DEF": ["Tagliafico", "Niakhaté", "Kluivert", "Maitland-Niles"],
        "MID": ["Tessmann", "Tolisso", "Sulc"], 
        "FWD": ["Lacazette", "Nuamah", "Fofana"] 
    },
    "ASM": {
        "GK": ["Hrádecký"], 
        "DEF": ["Vanderson", "Singo", "Teze", "Henrique"],
        "MID": ["Camara", "Pogba", "Zakaria"], 
        "FWD": ["Balogun", "Minamino", "Golovin"] 
    },
    "Man City": {
        "GK": ["Donnarumma"], 
        "DEF": ["Nunes", "Dias", "McAtee", "Gvardiol"],
        "MID": ["Rodri", "Cherki", "Bernardo", "Foden"], 
        "FWD": ["Haaland", "Doku"] 
    },
    "Liverpool": {
        "GK": ["Alisson"], 
        "DEF": ["Frimpong", "Van Dijk", "Konaté", "Robertson"],
        "MID": ["Mac Allister", "Szoboszlai", "Gravenberch"], 
        "FWD": ["Salah", "Ekitike", "Gakpo"] 
    },
    "Real Madrid": {
        "GK": ["Courtois"], 
        "DEF": ["Carvajal", "Militao", "Rudiger", "Mendy"],
        "MID": ["Valverde", "Tchouameni", "Bellingham"], 
        "FWD": ["Mbappé", "Vinicius Jr", "Rodrygo"] 
    },
    "Barca": {
        "GK": ["Garcia"], 
        "DEF": ["Koundé", "Cubarsi", "Araujo", "Balde"],
        "MID": ["De Jong", "Pedri", "Olmo"], 
        "FWD": ["Lewandowski", "Yamal", "Raphinha"] 
    },
    "Bayern": {
        "GK": ["Neuer"], 
        "DEF": ["Upamecano", "Kim", "Davies", "Guerreiro"],
        "MID": ["Kimmich", "Palhinha", "Musiala"], 
        "FWD": ["Kane", "Gnabry", "Olise"] 
    },
    "Dortmund": {
        "GK": ["Kobel"], 
        "DEF": ["Anton", "Schlotterbeck", "Ryerson", "Bensebaini"],
        "MID": ["Can", "Gross", "Brandt"], 
        "FWD": ["Guirassy", "Beier", "Adeyemi"] 
    }
}

# --- 4. GAME STATE CONFIGURATION ---
match_state = {
    m['id']: {
        'possession': m['home'], 
        'ball_position': 50,      
        'players': {},            
        'banned': [],             
        'score_home': 0, 
        'score_away': 0,
        'stoppage_time_1': random.randint(1, 2),
        'stoppage_time_2': random.randint(3, 6)
    } for m in ACTIVE_MATCHES
}

POSSESSION_WEIGHTS = {"GK": 10, "DEF": 35, "MID": 40, "FWD": 15}

# --- 5. xG FUNCTION ---
def calculate_xg(event_type, role, x_pos):
    if event_type == 'GOAL': return 1.0
    if event_type == 'PENALTY': return 0.79
    if event_type == 'SHOT':
        distance_factor = (x_pos / 100) ** 2.5
        base_xg = 0.10 
        if role == 'FWD': base_xg = 0.18 
        elif role == 'MID': base_xg = 0.08 
        shot_quality = random.uniform(0.6, 1.6)
        return round(min(base_xg * distance_factor * shot_quality * 4.5, 0.96), 2)
    return 0.0

# --- 6. UPDATE STATS ---
def update_stats(match_id, player, event_type, x_pos, active_team, match_info):
    state = match_state[match_id]
    
    if player not in state['players']:
        state['players'][player] = {'goals': 0, 'shots': 0, 'passes': 0, 'actions': 0, 'yellow_cards': 0, 'red_cards': 0}
    
    state['players'][player]['actions'] += 1
    
    if event_type == 'GOAL': 
        state['players'][player]['goals'] += 1
        if active_team == match_info['home']:
            state['score_home'] += 1
        else:
            state['score_away'] += 1
            
    elif event_type in ['SHOT', 'PENALTY']: state['players'][player]['shots'] += 1
    elif event_type == 'PASS': state['players'][player]['passes'] += 1
    elif event_type == 'CARD_YELLOW': state['players'][player]['yellow_cards'] += 1
    elif event_type == 'CARD_RED': state['players'][player]['red_cards'] += 1

# --- 7. GAME ENGINE ---
match_time = 0.0 
half_time_passed = False 
period = 1 

print(f"🏟️  STARTING REALISTIC MULTIPLEX (Tactics, Stars & Momentum)")
print(f"📡  Sending to Kafka ('{TOPIC_NAME}')")
print("----------------------------------------------------------------")

try:
    while True:
        # TIME MANAGEMENT
        current_max_time = 45 + match_state[ACTIVE_MATCHES[0]['id']]['stoppage_time_1'] if period == 1 else 90 + match_state[ACTIVE_MATCHES[0]['id']]['stoppage_time_2']
        
        if match_time > current_max_time:
            if period == 1:
                print(f"\n⏸️  HALF TIME ! ({int(match_time)}th minute)")
                time.sleep(2.0)
                print("▶️  SECOND HALF KICK-OFF\n")
                match_time = 45.0
                period = 2
                half_time_passed = True
                for mid in match_state:
                    match_state[mid]['ball_position'] = 50
                    home = next(m['home'] for m in ACTIVE_MATCHES if m['id'] == mid)
                    away = next(m['away'] for m in ACTIVE_MATCHES if m['id'] == mid)
                    match_state[mid]['possession'] = away 
                continue
            else:
                break 

        # SELECT MATCH
        match = random.choice(ACTIVE_MATCHES)
        mid = match['id']
        state = match_state[mid]
        poss_team = state['possession']
        def_team = match['away'] if poss_team == match['home'] else match['home']
        
        # --- TACTICAL ANALYSIS ---
        is_home_possession = (poss_team == match['home'])
        score_diff = state['score_home'] - state['score_away']
        desperation_mode = False
        park_the_bus = False
        
        if match_time > 75:
            if (is_home_possession and score_diff == -1) or (not is_home_possession and score_diff == 1):
                desperation_mode = True
            elif (is_home_possession and score_diff == 1) or (not is_home_possession and score_diff == -1):
                park_the_bus = True

        # --- ACTION DETERMINATION ---
        x = state['ball_position']
        event_type = "WAIT"
        player_name = "Unknown"
        active_team = poss_team 
        selected_pos = "MID"
        
        keep_ball_prob = 0.85 
        if x > 80: keep_ball_prob = 0.65
        
        if match_time > 70:
            keep_ball_prob -= 0.05 

        if random.random() < keep_ball_prob:
            # --- OFFENSIVE ACTION ---
            active_team = poss_team
            
            if x < 30: pos_w = {"GK": 20, "DEF": 70, "MID": 10, "FWD": 0}
            elif x < 60: pos_w = {"GK": 0, "DEF": 20, "MID": 70, "FWD": 10}
            else: pos_w = {"GK": 0, "DEF": 0, "MID": 40, "FWD": 60}
            
            selected_pos = random.choices(list(pos_w.keys()), weights=list(pos_w.values()))[0]
            player_name = random.choice(TEAMS_STRUCTURED[active_team][selected_pos])

            if selected_pos == "GK":
                event_type = "PASS"
                if random.random() < 0.6: 
                    state['ball_position'] = 50 + random.randint(-10, 10)
                    if random.random() < 0.5:
                        state['possession'] = def_team
                        state['ball_position'] = 100 - state['ball_position']
                        event_type = "AERIAL_DUEL"
            else:
                if x > 85: 
                    action_roll = random.random()
                    shot_threshold = 0.38
                    if desperation_mode: shot_threshold = 0.60 
                    if park_the_bus: shot_threshold = 0.10 
                    
                    if action_roll < shot_threshold: event_type = "SHOT"
                    elif action_roll < shot_threshold + 0.05: event_type = "FOUL_RECEIVED" 
                    else: event_type = "PASS"
                else:
                    long_shot_prob = 0.10
                    if desperation_mode: long_shot_prob = 0.30 
                    
                    if x > 70 and random.random() < long_shot_prob: event_type = "SHOT"
                    else: event_type = "PASS"
                
        else:
            # --- DEFENSIVE ACTION ---
            active_team = def_team
            action_roll = random.random()
            
            pressing_bonus = 0.0
            if (is_home_possession and score_diff == 1) or (not is_home_possession and score_diff == -1):
                 pressing_bonus = 0.2
            
            if action_roll < 0.7 - pressing_bonus: event_type = "INTERCEPTION"
            elif action_roll < 0.9: event_type = "TACKLE"
            else: event_type = "FOUL"
            
            def_x = 100 - x
            if def_x < 30: pos_w = {"GK": 80, "DEF": 20, "MID": 0, "FWD": 0} 
            elif def_x < 60: pos_w = {"GK": 0, "DEF": 80, "MID": 20, "FWD": 0}
            else: pos_w = {"GK": 0, "DEF": 0, "MID": 50, "FWD": 50} 
            
            selected_pos = random.choices(list(pos_w.keys()), weights=list(pos_w.values()))[0]
            player_name = random.choice(TEAMS_STRUCTURED[active_team][selected_pos])

        if player_name in state['banned']:
            match_time += 0.01; continue

        # --- RESOLUTION ---
        xg_value = 0.0
        log_message = ""
        
        if event_type == "PASS":
            progression = random.randint(-2, 15)
            if desperation_mode: progression = random.randint(5, 25)
            if park_the_bus: progression = random.randint(-10, 5)
            state['ball_position'] = max(0, min(99, x + progression))
            
        elif event_type == "AERIAL_DUEL":
             log_message = f"✈️ Aerial duel in midfield"
             
        elif event_type == "SHOT":
            is_on_target = random.random() < 0.45
            xg_value = calculate_xg("SHOT", selected_pos, x)
            if is_on_target:
                if random.random() < (xg_value * 1.2) + 0.05: 
                    event_type = "GOAL"
                    log_message = f"⚽ GOAAAAL !!! {player_name} ({active_team})"
                    state['possession'] = def_team
                    state['ball_position'] = 50
                else:
                    log_message = f"🚀 Great save by the keeper from {player_name}"
                    state['possession'] = def_team 
                    state['ball_position'] = 10 
            else:
                log_message = f"🚀 Shot off target by {player_name}"
                state['possession'] = def_team
                state['ball_position'] = 5 
                
        elif event_type == "FOUL_RECEIVED":
            def_player = random.choice(TEAMS_STRUCTURED[def_team]["DEF"])
            if x > 92: 
                event_type = "PENALTY"
                xg_value = 0.79
                log_message = f"⚠️ PENALTY for {poss_team} !!! Foul by {def_player}"
                
                # Star player takes the pen
                if poss_team in TEAMS_STRUCTURED and "FWD" in TEAMS_STRUCTURED[poss_team]:
                    shooter = TEAMS_STRUCTURED[poss_team]["FWD"][0] 
                else:
                    shooter = player_name
                
                player_name = shooter 
                active_team = poss_team

                if random.random() < 0.85: 
                    event_type = "GOAL"
                    log_message += f" -> Scored by {shooter} !"
                    state['possession'] = def_team
                    state['ball_position'] = 50
                else:
                    event_type = "SHOT" 
                    log_message += f" -> MISSED by {shooter} !"
                    state['possession'] = def_team
                    state['ball_position'] = 5
            else:
                event_type = "FOUL"
                active_team = def_team 
                player_name = def_player
                log_message = f"Foul by {player_name} ({active_team})"
                state['possession'] = poss_team
                
        elif event_type == "FOUL":
            severity = random.random()
            if severity < 0.05: 
                event_type = "CARD_RED"
                state['banned'].append(player_name)
                log_message = f"🟥 DIRECT RED CARD for {player_name} ({active_team}) !!!"
            elif severity < 0.35: 
                event_type = "CARD_YELLOW"
                log_message = f"🟨 Yellow Card for {player_name} ({active_team})"
            else:
                log_message = f"Foul against {player_name}"
            state['possession'] = poss_team 
            
        elif event_type in ["INTERCEPTION", "TACKLE"]:
            state['possession'] = def_team
            state['ball_position'] = 100 - x
            log_message = f"🦾 {event_type} by {player_name} ({active_team})"

        # --- SEND TO KAFKA ---
        current_minute = int(match_time)
        event = {
            'match_id': match['id'],
            'league': match['league'],
            'home_team': match['home'],
            'away_team': match['away'],
            'timestamp': int(time.time()),
            'game_time': current_minute,
            'event_type': event_type,
            'team': active_team,
            'player': player_name,
            'position_role': selected_pos,
            'coordinate_x': round(x, 1), 
            'xg': xg_value
        }
        
        producer.send(TOPIC_NAME, value=event)
        update_stats(mid, player_name, event_type, x, active_team, match)

        # --- LOGS ---
        time_display = f"[{current_minute}'][{match['league']}]"
        should_print = event_type in ["GOAL", "CARD_RED", "CARD_YELLOW", "PENALTY", "SHOT"] or (event_type != "PASS" and random.random() < 0.2)
        
        prefix = ""
        if desperation_mode and event_type in ["SHOT", "PASS"]: prefix = "🔥 [ASSAULT] "
        
        if log_message and should_print:
            print(f"{time_display} {prefix}{log_message}")
        elif event_type == "GOAL":
             print(f"{time_display} ⚽ GOAAAAL !!! {player_name} ({active_team})")
        
        match_time += random.uniform(0.05, 0.15) 
        time.sleep(random.uniform(0.05, 0.15))

    print("\n🏁  FULL TIME")
    print("----------------------------------------------------------------")
    print("🏆  FINAL RESULTS")
    for mid in match_state:
        home = next(m['home'] for m in ACTIVE_MATCHES if m['id'] == mid)
        away = next(m['away'] for m in ACTIVE_MATCHES if m['id'] == mid)
        s_h = match_state[mid]['score_home']
        s_a = match_state[mid]['score_away']
        print(f"   {home} {s_h} - {s_a} {away}")
        
    producer.close()

except KeyboardInterrupt:
    print("Stopped.")
    producer.close()