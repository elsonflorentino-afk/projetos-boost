#!/usr/bin/env python3
"""
generate_criativos_manus.py
Lê o CSV de copys da C8 e gera criativos via Manus AI API.

Uso:
  python3 scripts/generate_criativos_manus.py [--csv PATH] [--only N] [--profile speed|quality]

Env vars (obrigatórias):
  MANUS_API_KEY

Saída:
  - Tasks criadas no Manus com prompts pixel-perfect
  - Log com task_id e URLs para acompanhar
"""
import csv
import json
import os
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

# ── Config ──
MANUS_API_KEY = os.environ.get('MANUS_API_KEY')
if not MANUS_API_KEY:
    raise SystemExit('ERRO: defina MANUS_API_KEY no ambiente. Adicione ao .env e rode: export $(cat .env | xargs)')

MANUS_BASE = 'https://api.manus.ai/v2'
DEFAULT_CSV = 'docs/campanhas/boost-research/C8-maio-2026/COPYS_C8_v6.csv'
OUTPUT_DIR = 'criativos/c8-maio-2026'

# ── Fotos André por tom ──
FOTOS = {
    'confessional': 'IP1_9125.jpg',
    'casual': 'IP1_9130.jpg',
    'metodo': 'IP1_9210.jpg',
    'serio': 'IP1_9210.jpg',
    'provocacao': 'IP1_9170.jpg',
    'acessivel': 'IP1_9290.jpg',
    'escritorio': 'IP1_9130.jpg',
    'institucional': None,  # sem foto André, usar logo Boost
}

# ── Mapeamento copy -> tom da foto ──
COPY_FOTO_MAP = {
    '1': 'metodo',
    '2': 'confessional',
    '3': 'provocacao',
    '4': 'provocacao',
    '5': 'metodo',
    '6': 'confessional',
    '7': 'serio',
    '8': 'institucional',
    '9': 'casual',
    '10': 'casual',
}


def manus_request(endpoint, method='POST', data=None):
    """Faz request pra API Manus via curl (urllib bloqueado pela API)."""
    import subprocess
    url = f'{MANUS_BASE}/{endpoint}'
    cmd = ['curl', '-s', '-X', method, url,
           '-H', 'Content-Type: application/json',
           '-H', f'x-manus-api-key: {MANUS_API_KEY}']
    if data:
        cmd += ['-d', json.dumps(data, ensure_ascii=False)]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.stdout:
            return json.loads(result.stdout)
        print(f'  ERRO: resposta vazia. stderr: {result.stderr[:200]}')
        return None
    except Exception as e:
        print(f'  ERRO: {e}')
        return None


def build_prompt_static(row, foto_file):
    """Monta o prompt pro Manus gerar criativo estático."""
    headline = row['Headline']
    description = row['Description']
    angulo = row['Ângulo']
    kv = row['KV']
    num = row['#']

    foto_instruction = ''
    if foto_file:
        foto_instruction = f"""
FOTO DO ANDRÉ: Use a foto "{foto_file}" do André Franco como elemento principal.
O André deve ocupar ~40% do lado direito do criativo. Fundo escuro atrás dele."""
    else:
        foto_instruction = """
SEM FOTO: Este é um criativo institucional da Boost Research.
Use o logo da Boost Research como elemento central. Tom corporativo."""

    prompt = f"""Crie um criativo estático para Meta Ads da Boost Research.

SPECS:
- Formato 1: Feed 1080x1080px (1:1)
- Formato 2: Story 1080x1920px (9:16)
- Entregar AMBOS os formatos
- Formato: PNG, fundo escuro (#0A0E13 ou #12191D)
- Tipografia: Inter Bold para headline, Inter Regular para body
- Cores: verde accent #03E4D0, branco #F0F2F5, cinza #8899AA
- Logo Boost Research no canto inferior esquerdo (pequeno, discreto)
- MÁXIMO 7 palavras no headline visual (o resto vai no texto do ad)

CONTEÚDO:
- Headline visual: "{headline}"
- Subtítulo (menor): "{description}"
- Ângulo: {angulo}
- Tom: {kv}
{foto_instruction}

REFERÊNCIA DE ESTILO:
- Dark mode premium, similar a apps financeiros (Stripe, Linear)
- Sem emojis, sem ícones genéricos de cadeado/escudo
- Hierarquia visual clara: headline grande > subtítulo pequeno > logo discreto
- Contraste alto entre texto e fundo
- O criativo deve parecer premium e institucional, não infoproduto

REGRAS:
- NUNCA incluir promessa de retorno financeiro
- NUNCA usar "mentoria", "curso", "segredo"
- Tom sóbrio, profissional, de analista, não de influencer
- O headline deve ser legível mesmo em thumbnail 200x200px

Nomeie os arquivos como:
- C8-{num}-{angulo.replace(' ', '-').lower()}-feed-1080x1080.png
- C8-{num}-{angulo.replace(' ', '-').lower()}-story-1080x1920.png
"""
    return prompt


def build_prompt_video(row):
    """Monta prompt pro Manus gerar thumbnail de vídeo."""
    headline = row['Headline']
    num = row['#']
    angulo = row['Ângulo']

    prompt = f"""Crie uma thumbnail para vídeo Meta Ads da Boost Research.

SPECS:
- Formato 1: Feed 1080x1350px (4:5)
- Formato 2: Story 1080x1920px (9:16)
- Entregar AMBOS
- PNG, fundo escuro (#0A0E13)
- Tipografia: Inter Bold
- Cores: verde #03E4D0, branco, cinza

CONTEÚDO:
- Headline visual: "{headline}"
- Foto André Franco (IP1_9125.jpg) olhando direto pra câmera
- Play button sutil no centro (triângulo branco com 30% opacidade)
- Logo Boost Research discreto

ESTILO:
- Thumbnail de YouTube/Meta premium
- Dark mode, contraste alto
- Headline ocupa 1/3 superior
- André ocupa 2/3 inferior

Nomeie: C8-{num}-{angulo.replace(' ', '-').lower()}-thumb-feed.png e story.png
"""
    return prompt


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Gera criativos via Manus AI')
    parser.add_argument('--csv', default=DEFAULT_CSV, help='Path do CSV de copys')
    parser.add_argument('--only', type=int, default=0, help='Gerar só os primeiros N')
    parser.add_argument('--profile', default='speed', choices=['speed', 'quality'])
    parser.add_argument('--static-only', action='store_true', help='Só estáticos')
    parser.add_argument('--video-only', action='store_true', help='Só vídeos (thumbnails)')
    parser.add_argument('--dry-run', action='store_true', help='Só mostra prompts, não envia')
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f'ERRO: CSV não encontrado: {csv_path}')

    # Ler CSV
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f'CSV: {csv_path} ({len(rows)} copys)')
    print(f'Profile: {args.profile}')
    print(f'Dry run: {args.dry_run}')
    print()

    # Criar output dir
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    tasks = []
    for i, row in enumerate(rows):
        if args.only and i >= args.only:
            break

        num = row['#']
        tipo = row['Tipo']
        angulo = row['Ângulo']

        if tipo == 'ESTÁTICO' and args.video_only:
            continue
        if tipo == 'VÍDEO' and args.static_only:
            continue

        print(f'[{num}] {tipo} — {angulo}')

        if tipo == 'ESTÁTICO':
            tom = COPY_FOTO_MAP.get(num, 'casual')
            foto = FOTOS.get(tom)
            prompt = build_prompt_static(row, foto)
        else:
            prompt = build_prompt_video(row)

        if args.dry_run:
            print(f'  PROMPT ({len(prompt)} chars):')
            print(f'  {prompt[:200]}...')
            print()
            continue

        # Enviar pro Manus
        result = manus_request('task.create', data={
            'message': {'content': prompt},
        })

        if result and result.get('ok'):
            task_id = result['task_id']
            task_url = result['task_url']
            print(f'  Task criada: {task_id}')
            print(f'  URL: {task_url}')
            tasks.append({
                'copy_num': num,
                'tipo': tipo,
                'angulo': angulo,
                'task_id': task_id,
                'task_url': task_url,
                'status': 'created',
            })
            # Rate limit: esperar 2s entre tasks
            time.sleep(2)
        else:
            print(f'  FALHA ao criar task')

    # Salvar log
    if tasks:
        log_path = Path(OUTPUT_DIR) / 'manus_tasks.json'
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(tasks, f, indent=2, ensure_ascii=False)
        print(f'\n{len(tasks)} tasks criadas. Log salvo em: {log_path}')
        print('\nPara verificar status:')
        print(f'  python3 {sys.argv[0]} --check')
    elif not args.dry_run:
        print('\nNenhuma task criada.')


if __name__ == '__main__':
    main()
