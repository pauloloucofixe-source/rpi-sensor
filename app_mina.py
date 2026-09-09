from flask import Flask, render_template_string, jsonify, request
from gpiozero import DistanceSensor, OutputDevice
from datetime import datetime, timedelta
import threading
import logging
import json
import os
from time import sleep
import requests

# 1. LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/home/pi/mina.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("mina")

# 2. CONFIGURAÇÕES E HARDWARE

TELEGRAM_TOKEN   = "8677664634:AAF3t_LgzSHxNhLk_bOeixy96W3q2g47NCg"
TELEGRAM_CHAT_ID = "7853234143"

FICHEIRO_CONFIG    = "/home/pi/medidas.txt"
FICHEIRO_HISTORICO = "/home/pi/historico.json"
FICHEIRO_EVENTOS   = "/home/pi/eventos.json"

medidas = {
    "fundo": 0.35, "limite": 0.20, "largura": 0.15, "comprimento": 0.15,
    "horas": [3, 6, 11, 15, 18, 23]
}
historico_24h = []
eventos_log   = []
_lock         = threading.Lock()
modo_rega     = "AUTO" 

em_recuperacao_furo = False

def carregar_dados():
    global medidas, historico_24h, eventos_log
    if os.path.exists(FICHEIRO_CONFIG):
        try:
            with open(FICHEIRO_CONFIG, 'r') as f:
                dados = json.load(f)
                medidas.update(dados)
        except Exception as e: log.warning(f"Erro config: {e}")
    if os.path.exists(FICHEIRO_HISTORICO):
        try:
            with open(FICHEIRO_HISTORICO, 'r') as f: historico_24h = json.load(f)
        except: historico_24h = []
    if os.path.exists(FICHEIRO_EVENTOS):
        try:
            with open(FICHEIRO_EVENTOS, 'r') as f: eventos_log = json.load(f)
        except: eventos_log = []

def guardar_config():
    try:
        with open(FICHEIRO_CONFIG, 'w') as f: json.dump(medidas, f)
    except Exception as e: log.error(f"Erro config: {e}")

def guardar_historico():
    try:
        with open(FICHEIRO_HISTORICO, 'w') as f: json.dump(historico_24h, f)
    except: pass

def registar_evento(msg, tipo="info"):
    global eventos_log
    agora = datetime.now()
    entrada = {"ts": agora.timestamp(), "hora": agora.strftime("%H:%M:%S"), "msg": msg, "tipo": tipo}
    with _lock:
        eventos_log.append(entrada)
        corte = (agora - timedelta(hours=24)).timestamp()
        eventos_log = [e for e in eventos_log if e.get("ts", 0) >= corte]
    try:
        with open(FICHEIRO_EVENTOS, 'w') as f: json.dump(eventos_log, f)
    except: pass

carregar_dados()

sensor       = DistanceSensor(echo=24, trigger=23, max_distance=3.0)
valvula_furo = OutputDevice(17, active_high=False, initial_value=False)
bomba_rega   = OutputDevice(27, active_high=False, initial_value=False)

dados_web = {"pct": 0.0, "litros": 0, "estado_furo": "DESLIGADO", "estado_rega": "CORTADA", "last_update": ""}
hora_fim_furo = None

# 3. LOGICA DE FUNDO

def avisar_telegram(msg):
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=3)
    except: pass

def vigiar_mina():
    global dados_web, historico_24h, hora_fim_furo, modo_rega, em_recuperacao_furo
    ultima_gravacao_hist = datetime.now() - timedelta(minutes=16)
    ultima_hora_agendada = -1
    registar_evento("Sistema iniciado", "info")

    while True:
        try:
            agora = datetime.now()
            dist_m = sensor.distance
            if dist_m is None: sleep(2); continue

            alt_max = medidas["fundo"] - medidas["limite"]
            if alt_max <= 0: sleep(5); continue

            alt_atual = max(0.0, medidas["fundo"] - dist_m)
            pct       = round(min(100.0, (alt_atual / alt_max) * 100), 1)
            litros    = int((medidas["largura"] * medidas["comprimento"] * alt_atual) * 1000)

            if (agora - ultima_gravacao_hist).total_seconds() >= 900:
                historico_24h.append({"hora": agora.strftime("%H:%M"), "ts": agora.timestamp(), "val": pct})
                corte = (agora - timedelta(hours=24)).timestamp()
                historico_24h = [e for e in historico_24h if e.get("ts", 0) >= corte]
                guardar_historico()
                ultima_gravacao_hist = agora

            # 1. Ativação por nível Crítico (<= 10%)
            if pct <= 10.0 and not em_recuperacao_furo:
                em_recuperacao_furo = True
                hora_fim_furo = None 
                valvula_furo.on()
                registar_evento(f"⚠️ Nível crítico ({pct}%)! Bomba cortada. Furo ligado até aos 20%.", "warn")
                avisar_telegram(f"⚠️ Alerta Mina: Nível crítico de {pct}%. Bomba desligada. A repor água com o furo.")

            # 2. Desativação após atingir a meta (>= 20%)
            if em_recuperacao_furo and pct >= 20.0:
                em_recuperacao_furo = False
                valvula_furo.off()
                registar_evento(f" Enchimento concluído ({pct}%). Furo desligado.", "info")
                avisar_telegram(f" Mina Recuperada: Nível atingiu {pct}%. Furo desligado. Sistema normalizado.")

            # 3. Agendamentos Horários Normais
            if not em_recuperacao_furo:
                if agora.hour in medidas.get("horas", []) and agora.minute == 0 and agora.hour != ultima_hora_agendada:
                    ultima_hora_agendada = agora.hour
                    if pct < 95.0:
                        with _lock: hora_fim_furo = agora + timedelta(minutes=10)
                        valvula_furo.on()
                        registar_evento(f"Furo ligado (Agendamento {agora.hour}h por 10 min)", "ok")

                if valvula_furo.value and hora_fim_furo:
                    if pct >= 95.0 or agora >= hora_fim_furo:
                        valvula_furo.off()
                        with _lock: hora_fim_furo = None
                        registar_evento(f"Furo desligado pós-agendamento — {pct}%", "info")

            # --- LÓGICA DA BOMBA DE REGA ---
            estado_atual_rega = "CORTADA"
            
            if pct <= 10.0:
                bomba_rega.off()
                estado_atual_rega = "CORTADA (Segurança 10%)"
            elif pct < 13.0 and not bomba_rega.value:
                bomba_rega.off()
                estado_atual_rega = "A aguardar nível (Mín. 13%)"
            else:
                if modo_rega == "AUTO":
                    if not bomba_rega.value: 
                        bomba_rega.on()
                        registar_evento(f"💦 Nível recuperado ({pct}%). Bomba ligada em Auto.", "ok")
                    estado_atual_rega = "LIGADA (Auto)"
                elif modo_rega == "MANUAL_ON":
                    if not bomba_rega.value: bomba_rega.on()
                    estado_atual_rega = "FORÇADA (Manual)"
                elif modo_rega == "MANUAL_OFF":
                    if bomba_rega.value: bomba_rega.off()
                    estado_atual_rega = "PARADA (Manual)"

            with _lock:
                dados_web.update({
                    "pct": pct, 
                    "litros": litros, 
                    "estado_furo": "LIGADO (Emergência)" if em_recuperacao_furo else ("LIGADO" if valvula_furo.value else "DESLIGADO"), 
                    "estado_rega": estado_atual_rega, 
                    "last_update": agora.strftime("%H:%M:%S")
                })

        except Exception as e: log.error(f"Erro no ciclo: {e}")
        sleep(2)

# 4. FLASK WEB APP & API

app = Flask(__name__)

HTML_GUI = """
<!DOCTYPE html>
<html lang="pt">
<head>
    <meta charset="UTF-8">
    <title>Mina Inteligente</title>
    <meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0">
    
    <meta name="apple-mobile-web-app-capable" content="yes">
    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
    <link rel="manifest" href="/manifest.json">
    <link rel="apple-touch-icon" href="/icon">

    <link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root { 
            --bg-grad1: #050914; --bg-grad2: #0a1128;
            --surface: rgba(17, 24, 39, 0.65); 
            --surface2: rgba(26, 34, 54, 0.5); 
            --border: rgba(0, 212, 255, 0.15); 
            --cyan: #00d4ff; --cyan-dim: rgba(0,212,255,0.15); --cyan-glow: rgba(0,212,255,0.5); 
            --green: #00ffaa; --green-glow: rgba(0,255,170,0.4);
            --red: #ff3355; --red-glow: rgba(255,51,85,0.4);
            --amber: #ffb800; --text: #e2e8f0; --text-dim: #8b9bb4; 
            --mono: 'Space Mono', monospace; --sans: 'DM Sans', sans-serif; 
        }
        
        * { box-sizing: border-box; margin: 0; padding: 0; }
        
        /* Custom Scrollbar for a futuristic look */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: rgba(0,0,0,0.2); border-radius: 4px; }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: var(--cyan); }

        body { 
            font-family: var(--sans); 
            background: radial-gradient(circle at top right, var(--bg-grad2), var(--bg-grad1));
            color: var(--text); padding-bottom: 40px; 
            -webkit-tap-highlight-color: transparent;
            min-height: 100vh;
        }
        
        header { 
            background: rgba(10, 15, 30, 0.8); 
            backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px);
            border-bottom: 1px solid var(--border); 
            padding: 16px 24px; display: flex; align-items: center; justify-content: space-between; 
            box-shadow: 0 4px 30px rgba(0, 0, 0, 0.5);
        }
        .logo { display: flex; align-items: center; gap: 12px; }
        .logo-icon { width: 36px; height: 36px; background: var(--cyan-dim); border: 1px solid var(--cyan); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 18px; box-shadow: 0 0 10px var(--cyan-glow); }
        .logo-text { font-family: var(--mono); font-size: 14px; font-weight: 700; color: var(--text); letter-spacing:.08em; text-shadow: 0 0 8px rgba(255,255,255,0.2); }
        .logo-sub { font-family: var(--mono); font-size: 10px; color: var(--cyan); margin-top: 2px; letter-spacing: 0.05em; }
        
        .grid { max-width: 960px; margin: 24px auto; padding: 0 16px; display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
        @media (max-width: 640px) { .grid { grid-template-columns: 1fr; } }
        .span2 { grid-column: span 2; }
        @media (max-width: 640px) { .span2 { grid-column: span 1; } }
        
        .card { 
            background: var(--surface); 
            backdrop-filter: blur(16px); -webkit-backdrop-filter: blur(16px);
            border: 1px solid var(--border); 
            border-radius: 16px; padding: 22px; 
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
            position: relative; overflow: hidden;
        }
        /* Decorative corner accent */
        .card::before {
            content: ''; position: absolute; top: 0; left: 0; width: 30px; height: 30px;
            border-top: 2px solid var(--cyan); border-left: 2px solid var(--cyan);
            opacity: 0.5; border-top-left-radius: 16px;
        }
        
        .card-title { font-size: 11px; font-weight: 700; font-family: var(--mono); letter-spacing: .15em; text-transform: uppercase; color: var(--cyan); margin-bottom: 18px; display: flex; align-items: center; gap: 8px; }
        .card-title::after { content: ''; flex: 1; height: 1px; background: linear-gradient(90deg, var(--border), transparent); }

        .tank-wrap { display: flex; align-items: flex-end; justify-content: center; gap: 28px; }
        .tank-outer { width: 88px; height: 160px; border: 2px solid rgba(0, 212, 255, 0.3); box-shadow: 0 0 15px var(--cyan-dim) inset; border-radius: 6px 6px 3px 3px; position: relative; background: rgba(0,0,0,0.4); overflow: hidden; }
        .tank-fill { position: absolute; bottom: 0; left: 0; right: 0; background: linear-gradient(to top, #0033aa, var(--cyan)); box-shadow: 0 -5px 20px var(--cyan-glow); transition: height 1s cubic-bezier(.4,0,.2,1); }
        .tank-wave { width: 200%; height: 18px; margin-top: -9px; margin-left: -50%; background: rgba(255,255,255,.15); border-radius: 50%; animation: wave 2.5s linear infinite; }
        @keyframes wave { 0% { transform: translateX(0); } 100% { transform: translateX(50%); } }
        
        .tank-ticks { display: flex; flex-direction: column; justify-content: space-between; height: 160px; padding: 2px 0; }
        .tick { font-size: 10px; color: var(--text-dim); font-family: var(--mono); position: relative; display: flex; align-items: center; gap: 5px; }
        .tick::after { content: ''; width: 8px; height: 1px; background: var(--border); }
        
        .big-pct { font-family: var(--mono); font-size: 56px; font-weight: 700; line-height: 1; color: #fff; text-shadow: 0 0 20px var(--cyan-glow), 0 0 40px var(--cyan-glow); transition: all .5s; }
        .big-pct.warn { color: var(--amber); text-shadow: 0 0 20px rgba(255,184,0,.5); }
        .big-pct.crit { color: var(--red); text-shadow: 0 0 20px rgba(255,51,85,.6); }
        .big-unit { font-size: 20px; color: var(--cyan); margin-left: 2px; text-shadow: none; }
        .litros-val { font-family: var(--mono); font-size: 20px; color: var(--text-dim); margin-top: 8px; letter-spacing: 0.05em; }
        .litros-val span { color: var(--cyan); text-shadow: 0 0 10px var(--cyan-glow); }

        .status-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 18px; }
        .status-pill { background: rgba(0,0,0,0.3); border: 1px solid var(--border); border-radius: 12px; padding: 14px; display: flex; align-items: center; gap: 12px; transition: all 0.3s; }
        .status-pill:hover { border-color: rgba(0,212,255,0.4); background: rgba(0,212,255,0.05); }
        
        .status-led { width: 12px; height: 12px; border-radius: 50%; flex-shrink: 0; position: relative; }
        .status-led::after { content: ''; position: absolute; top: -4px; left: -4px; right: -4px; bottom: -4px; border-radius: 50%; opacity: 0.5; }
        .led-on { background: var(--green); box-shadow: 0 0 12px var(--green); }
        .led-on::after { background: var(--green); animation: pulse 2s infinite; }
        .led-off { background: rgba(255,255,255,0.1); border: 1px solid rgba(255,255,255,0.2); }
        .led-warn { background: var(--amber); box-shadow: 0 0 12px var(--amber); }
        .led-warn::after { background: var(--amber); animation: pulse 1s infinite; }
        @keyframes pulse { 0% { transform: scale(1); opacity: 0.6; } 100% { transform: scale(2); opacity: 0; } }
        
        .pill-label { font-size: 10px; color: var(--text-dim); font-family: var(--mono); font-weight: 500; letter-spacing:.1em; text-transform: uppercase; }
        .pill-val { font-size: 13px; font-weight: 700; font-family: var(--sans); margin-top: 2px; color: #fff; }
        
        .btn-group { display: flex; flex-direction: column; gap: 10px; }
        .btn { 
            background: transparent; border: 1px solid; border-radius: 8px; padding: 14px 16px; 
            font-family: var(--mono); font-size: 12px; font-weight: 700; letter-spacing: 0.05em; text-transform: uppercase;
            cursor: pointer; display: flex; align-items: center; justify-content: center; gap: 8px; 
            transition: all 0.3s ease; position: relative; overflow: hidden;
        }
        
        .btn-cyan { border-color: var(--cyan); color: var(--cyan); box-shadow: 0 0 15px var(--cyan-dim) inset; }
        .btn-cyan:hover { background: var(--cyan); color: #000; box-shadow: 0 0 20px var(--cyan-glow); }
        
        .btn-red { border-color: var(--red); color: var(--red); box-shadow: 0 0 15px rgba(255,51,85,0.15) inset; }
        .btn-red:hover { background: var(--red); color: #fff; box-shadow: 0 0 20px var(--red-glow); }
        
        .btn-green { border-color: var(--green); color: var(--green); box-shadow: 0 0 15px rgba(0,255,170,0.15) inset; }
        .btn-green:hover { background: var(--green); color: #000; box-shadow: 0 0 20px var(--green-glow); }
        
        .btn-neutral { border-color: var(--text-dim); color: var(--text); background: rgba(255,255,255,0.05); }
        .btn-neutral:hover { border-color: #fff; background: rgba(255,255,255,0.1); }
        
        .chart-wrap { position: relative; height: 180px; }
        .log-scroll { height: 155px; overflow-y: auto; background: rgba(0,0,0,0.3); border: 1px solid var(--border); border-radius: 8px; padding: 12px; font-family: var(--mono); font-size: 11px; }
        .log-row { padding: 6px 0; border-bottom: 1px solid rgba(0,212,255,0.1); color: var(--text-dim); display: flex; gap: 12px; }
        .log-row:last-child { border-bottom: none; }
        .log-row.ok .msg { color: var(--green); text-shadow: 0 0 5px rgba(0,255,170,0.3); }
        .log-row.warn .msg { color: var(--amber); }
        .log-row .hora { color: var(--cyan); opacity: 0.7; }

        .metric-badges-wrap { display: flex; gap: 8px; margin-bottom: 15px; }
        .metric-badge { background: rgba(0,0,0,0.3); border: 1px solid rgba(0,212,255,0.2); border-radius: 8px; padding: 10px 5px; flex: 1; text-align: center; display: flex; flex-direction: column; gap: 6px; }
        .metric-badge .pill-label { font-size: 9px; color: var(--text-dim); font-family: var(--mono); }
        .metric-badge .pill-val { font-size: 14px; color: var(--cyan); font-family: var(--mono); text-shadow: 0 0 8px var(--cyan-glow); font-weight: 700; }
        
        .divider { height: 1px; background: linear-gradient(90deg, transparent, var(--border), transparent); margin: 22px 0; }

        .cfg-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
        .cfg-field label { font-size: 10px; color: var(--text-dim); font-family: var(--mono); font-weight: 700; letter-spacing:.08em; text-transform: uppercase; display: flex; justify-content: space-between; margin-bottom: 8px; }
        .btn-calibrar { background: transparent; border: 1px solid var(--cyan); color: var(--cyan); border-radius: 4px; padding: 2px 8px; font-size: 9px; cursor: pointer; transition: all 0.2s; font-family: var(--mono); }
        .btn-calibrar:hover { background: var(--cyan); color: #000; }
        
        .cfg-field input { width: 100%; padding: 12px; background: rgba(0,0,0,0.4); border: 1px solid var(--border); border-radius: 8px; color: var(--cyan); font-family: var(--mono); font-size: 14px; transition: all 0.3s; }
        .cfg-field input:focus { outline: none; border-color: var(--cyan); box-shadow: 0 0 15px var(--cyan-dim); background: rgba(0,212,255,0.05); }
    </style>
</head>
<body>

<header>
    <div class="logo">
        <div class="logo-icon">💧</div>
        <div>
            <div class="logo-text">Mina Casa</div>
            <div class="logo-sub" id="update-ts">A SINCRONIZAR...</div>
        </div>
    </div>
</header>

<div class="grid">
    <div class="card">
        <div class="card-title">Nível da Água</div>
        <div class="tank-wrap">
            <div class="tank-ticks"><span class="tick">100%</span><span class="tick">50%</span><span class="tick">0%</span></div>
            <div class="tank-outer">
                <div class="tank-fill" id="tank-fill" style="height:0%"><div class="tank-wave"></div></div>
            </div>
            <div>
                <div class="big-pct" id="p-lvl">0<span class="big-unit">%</span></div>
                <div class="litros-val" id="l-lvl">VOL: <span>0</span> L</div>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="card-title">MÓDULOS CONTROLO</div>
        <div class="status-grid">
            <div class="status-pill"><div class="status-led" id="led-furo"></div><div><div class="pill-label">BOMBA_FURO</div><div class="pill-val" id="s-furo">--</div></div></div>
            <div class="status-pill"><div class="status-led" id="led-rega"></div><div><div class="pill-label">SISTEMA_REGA</div><div class="pill-val" id="s-rega">--</div></div></div>
        </div>
        <div class="btn-group">
            <button class="btn btn-cyan" id="btn-furo-on" onclick="cmdAPI('/api/comando_furo', 'ligar')">⚡ INICIAR FURO</button>
            <button class="btn btn-red" id="btn-furo-off" onclick="cmdAPI('/api/comando_furo', 'parar')" style="display:none">⛔ ABORTAR FURO</button>
            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; margin-top: 8px;">
                <button class="btn btn-green" onclick="cmdAPI('/api/comando_bomba', 'ligar')">💦 FORÇAR</button>
                <button class="btn btn-red" onclick="cmdAPI('/api/comando_bomba', 'parar')">⏹ PARAR</button>
                <button class="btn btn-neutral" onclick="cmdAPI('/api/comando_bomba', 'auto')">🤖 AUTO</button>
            </div>
        </div>
    </div>

    <div class="card span2">
        <div class="card-title">ANÁLISE 24H</div>
        <div class="chart-wrap"><canvas id="grafico24h"></canvas></div>
    </div>

    <div class="card">
        <div class="card-title">LOG DO SISTEMA</div>
        <div class="log-scroll" id="log-box"></div>
    </div>

    <div class="card">
        <div class="card-title">PARÂMETROS FÍSICOS</div>
        
        <div class="metric-badges-wrap">
            <div class="metric-badge"><span class="pill-label">FUNDO</span><span class="pill-val" id="disp-f">-- cm</span></div>
            <div class="metric-badge"><span class="pill-label">Z.CEGA</span><span class="pill-val" id="disp-z">-- cm</span></div>
            <div class="metric-badge"><span class="pill-label">LARG.</span><span class="pill-val" id="disp-l">-- cm</span></div>
            <div class="metric-badge"><span class="pill-label">COMP.</span><span class="pill-val" id="disp-c">-- cm</span></div>
        </div>
        
        <div class="divider"></div>

        <div class="cfg-grid">
            <div class="cfg-field">
                <label>Fundo (m) <button class="btn-calibrar" onclick="calibrarSensor('fundo')">🎯 SET</button></label>
                <input type="number" id="cfg-f" step="0.01">
            </div>
            <div class="cfg-field">
                <label>Z. Cega (m) <button class="btn-calibrar" onclick="calibrarSensor('limite')">🎯 SET</button></label>
                <input type="number" id="cfg-z" step="0.01">
            </div>
            <div class="cfg-field"><label>Largura (m)</label><input type="number" id="cfg-l" step="0.01"></div>
            <div class="cfg-field"><label>Comp. (m)</label><input type="number" id="cfg-c" step="0.01"></div>
            <div class="cfg-field" style="grid-column: span 2;"><label>Horas do Furo</label><input type="text" id="cfg-h" placeholder="Ex: 3, 6, 18"></div>
        </div>
        <button class="btn btn-neutral" style="width: 100%; margin-top: 18px;" onclick="saveCfg()">💾 GRAVAR NA MEMÓRIA</button>
    </div>
</div>

<script>
    function myFetch(url, params = {}) {
        params.headers = { ...params.headers, 'Content-Type': 'application/json' };
        return fetch(url, params).then(r => r.json());
    }

    function loadConfig() {
        myFetch('/api/config').then(d => {
            document.getElementById('cfg-f').value = d.fundo; 
            document.getElementById('cfg-z').value = d.limite;
            document.getElementById('cfg-l').value = d.largura; 
            document.getElementById('cfg-c').value = d.comprimento;
            document.getElementById('cfg-h').value = d.horas.join(', ');
            
            document.getElementById('disp-f').innerText = Math.round(d.fundo * 100) + ' cm';
            document.getElementById('disp-z').innerText = Math.round(d.limite * 100) + ' cm';
            document.getElementById('disp-l').innerText = Math.round(d.largura * 100) + ' cm';
            document.getElementById('disp-c').innerText = Math.round(d.comprimento * 100) + ' cm';
        });
    }

    function calibrarSensor(tipo) {
        if(confirm('Atenção: O tanque está no nível que desejas guardar como ' + tipo + '?')) {
            myFetch('/api/calibrar', { method: 'POST', body: JSON.stringify({tipo}) })
            .then(d => {
                if(d.s) {
                    loadConfig();
                    alert('Calibração aceite. Memória atualizada.');
                } else alert('Falha na leitura do sensor. Verifica o hardware.');
            });
        }
    }

    function cmdAPI(url, acao) {
        myFetch(url, { method: 'POST', body: JSON.stringify({acao}) }).then(carregarEventos);
    }

    function saveCfg() {
        const c = { 
            fundo: parseFloat(document.getElementById('cfg-f').value), limite: parseFloat(document.getElementById('cfg-z').value),
            largura: parseFloat(document.getElementById('cfg-l').value), comprimento: parseFloat(document.getElementById('cfg-c').value),
            horas: document.getElementById('cfg-h').value.split(',').map(n => parseInt(n)).filter(n => !isNaN(n))
        };
        myFetch('/api/config', { method: 'POST', body: JSON.stringify(c) }).then(() => { 
            alert("Parâmetros guardados no sistema!"); 
            loadConfig();
            carregarEventos(); 
        });
    }

    let grafico;
    function initGrafico() {
        Chart.defaults.color = '#8b9bb4';
        Chart.defaults.font.family = "'Space Mono', monospace";
        
        grafico = new Chart(document.getElementById('grafico24h').getContext('2d'), {
            type: 'line',
            data: { 
                labels: [], 
                datasets: [{ 
                    label: 'Nível (%)', 
                    data: [], 
                    borderColor: '#00d4ff', 
                    borderWidth: 2, 
                    fill: true, 
                    backgroundColor: 'rgba(0,212,255,0.08)', 
                    pointRadius: 0, 
                    pointHoverRadius: 6,
                    pointHoverBackgroundColor: '#fff',
                    tension: 0.4 
                }] 
            },
            options: { 
                responsive: true, 
                maintainAspectRatio: false, 
                interaction: { mode: 'index', intersect: false },
                plugins: { legend: { display: false }, tooltip: { backgroundColor: 'rgba(10,15,30,0.9)', titleFont: {size: 11}, bodyFont: {size: 13, weight: 'bold'}, displayColors: false } }, 
                scales: { 
                    y: { min: 0, max: 100, grid: { color: 'rgba(0,212,255,0.05)' }, border: { dash: [4, 4] } }, 
                    x: { grid: { color: 'rgba(0,212,255,0.05)' }, border: { display: false }, ticks: { maxTicksLimit: 6, font: {size: 9} } } 
                } 
            }
        });
    }

    function updateTodos() {
        myFetch('/api/status').then(d => {
            document.getElementById('tank-fill').style.height = d.pct + '%';
            document.getElementById('p-lvl').innerHTML = d.pct + '<span class="big-unit">%</span>';
            document.getElementById('p-lvl').className = 'big-pct' + (d.pct <= 10 ? ' crit' : d.pct <= 25 ? ' warn' : '');
            document.getElementById('l-lvl').innerHTML = 'VOL: <span>' + d.litros + '</span> L';
            document.getElementById('update-ts').innerText = 'TS: ' + d.last_update;
            document.getElementById('s-furo').innerText = d.estado_furo;
            document.getElementById('s-rega').innerText = d.estado_rega;
            
            document.getElementById('led-furo').className = 'status-led ' + (d.estado_furo.startsWith('LIGADO (Emergência)') ? 'led-warn' : (d.estado_furo == 'LIGADO' ? 'led-on' : 'led-off'));
            document.getElementById('led-rega').className = 'status-led ' + (d.estado_rega.startsWith('LIGADA') || d.estado_rega.startsWith('FORÇADA') ? 'led-on' : 'led-off');
            
            document.getElementById('btn-furo-on').style.display = d.estado_furo.startsWith('LIGADO') ? 'none' : 'flex';
            document.getElementById('btn-furo-off').style.display = d.estado_furo.startsWith('LIGADO') ? 'flex' : 'none';
        });

        myFetch('/api/historico').then(hist => {
            grafico.data.labels = hist.map(e => e.hora);
            grafico.data.datasets[0].data = hist.map(e => e.val);
            grafico.update('none');
        });
    }

    function carregarEventos() {
        myFetch('/api/eventos').then(evs => {
            document.getElementById('log-box').innerHTML = evs.map(e => `<div class="log-row ${e.tipo||''}"><span class="hora">[${e.hora}]</span><span class="msg">${e.msg}</span></div>`).join('');
        });
    }

    initGrafico();
    loadConfig();
    updateTodos();
    carregarEventos();
    setInterval(updateTodos, 3000);
    setInterval(carregarEventos, 5000);
</script>
</body>
</html>
"""

@app.route('/')
def home(): return render_template_string(HTML_GUI)

@app.route('/manifest.json')
def pwa_manifest():
    return jsonify({
        "name": "Mina Inteligente", "short_name": "Mina", "start_url": "/", "display": "standalone",
        "background_color": "#050914", "theme_color": "#050914",
        "icons": [{"src": "/icon", "sizes": "192x192", "type": "image/svg+xml"}]
    })

@app.route('/icon')
def pwa_icon():
    svg = "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><rect width='100' height='100' rx='20' fill='#050914'/><rect width='96' height='96' x='2' y='2' rx='18' fill='none' stroke='#00d4ff' stroke-width='2'/><text x='50' y='65' font-size='50' text-anchor='middle'>💧</text></svg>"
    return svg, 200, {'Content-Type': 'image/svg+xml'}

@app.route('/api/status')
def api_status():
    with _lock: return jsonify(dict(dados_web))

@app.route('/api/historico')
def api_historico(): return jsonify(historico_24h)

@app.route('/api/config', methods=['GET','POST'])
def api_config():
    global medidas
    if request.method == 'POST':
        medidas.update(request.json); guardar_config()
        registar_evento("Configurações Guardadas", "ok")
        return jsonify({"s": True})
    return jsonify(medidas)

@app.route('/api/calibrar', methods=['POST'])
def api_calibrar():
    tipo = request.json.get("tipo")
    dist = sensor.distance
    if dist is None or dist < 0.02: return jsonify({"s": False}), 400
    val_arredondado = round(dist, 2)
    medidas[tipo] = val_arredondado
    guardar_config()
    registar_evento(f"Sensor Calibrado ({tipo}): {val_arredondado}m", "info")
    return jsonify({"s": True, "val": val_arredondado})

@app.route('/api/eventos')
def api_eventos():
    with _lock: return jsonify(list(reversed(eventos_log)))

@app.route('/api/comando_furo', methods=['POST'])
def api_comando_furo():
    global hora_fim_furo, em_recuperacao_furo
    acao = request.json.get("acao", "ligar")
    if acao == "ligar":
        em_recuperacao_furo = False 
        with _lock: hora_fim_furo = datetime.now() + timedelta(minutes=10)
        valvula_furo.on()
        registar_evento("Furo ligado manualmente via App (10 min)", "ok")
    elif acao == "parar":
        em_recuperacao_furo = False
        valvula_furo.off()
        with _lock: hora_fim_furo = None
        registar_evento("Furo parado manualmente via App", "info")
    return jsonify({"s": True})

@app.route('/api/comando_bomba', methods=['POST'])
def api_comando_bomba():
    global modo_rega
    acao = request.json.get("acao", "auto")
    if acao == "ligar": modo_rega = "MANUAL_ON"; registar_evento("Rega: FORÇADA LIGAR", "warn")
    elif acao == "parar": modo_rega = "MANUAL_OFF"; registar_evento("Rega: FORÇADA PARAR", "warn")
    elif acao == "auto": modo_rega = "AUTO"; registar_evento("Rega: MODO AUTO", "ok")
    return jsonify({"s": True})

if __name__ == '__main__':
    threading.Thread(target=vigiar_mina, daemon=True).start()
    app.run(host='0.0.0.0', port=5000)
