// =============================================================================
// Google Apps Script — Sincronizacao Supabase -> Google Sheets
// Projeto: Roseli Andrade — Conversoes Offline para Google Ads
//
// Este script busca leads do Supabase (wa_conversions) e popula duas abas
// na planilha no formato exato que o Google Ads espera para upload manual
// ou automatizado de conversoes offline.
// =============================================================================

// ---------------------------------------------------------------------------
// Configuracao do Supabase
// ---------------------------------------------------------------------------
var SUPABASE_URL = 'https://xjngdyhzpgktddxfmdgq.supabase.co';
var SUPABASE_ANON_KEY = 'sb_publishable_gS5lC6Jhugq73v-_MMzM7g_GGWFDNZc';
var SUPABASE_AUTH_EMAIL = 'dashboard@clinicaroseli.local';
var SUPABASE_AUTH_PASSWORD = 'roseli2026!';

// Nomes das abas na planilha
var ABA_CONVERSOES = 'Conversoes';
var ABA_LOG = 'Log';

// ---------------------------------------------------------------------------
// Funcao auxiliar: faz login no Supabase Auth e retorna o JWT (access_token)
// ---------------------------------------------------------------------------
function getSupabaseToken() {
  var url = SUPABASE_URL + '/auth/v1/token?grant_type=password';

  var payload = {
    email: SUPABASE_AUTH_EMAIL,
    password: SUPABASE_AUTH_PASSWORD
  };

  var options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'apikey': SUPABASE_ANON_KEY
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  var response = UrlFetchApp.fetch(url, options);
  var code = response.getResponseCode();

  if (code !== 200) {
    throw new Error('Falha no login Supabase. HTTP ' + code + ': ' + response.getContentText());
  }

  var data = JSON.parse(response.getContentText());
  return data.access_token;
}

// ---------------------------------------------------------------------------
// Funcao auxiliar: busca leads do Supabase com os filtros necessarios
// Retorna array de objetos com gclid, status, updated_at, valor_consulta
// ---------------------------------------------------------------------------
function fetchConversions(token) {
  // Busca leads com status agendamento, compareceu ou nao_compareceu
  // que possuam gclid preenchido
  var url = SUPABASE_URL + '/rest/v1/wa_conversions'
    + '?status=in.(agendamento,compareceu,nao_compareceu)'
    + '&gclid=not.is.null'
    + '&select=gclid,status,updated_at,valor_consulta';

  var options = {
    method: 'get',
    headers: {
      'apikey': SUPABASE_ANON_KEY,
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/json'
    },
    muteHttpExceptions: true
  };

  var response = UrlFetchApp.fetch(url, options);
  var code = response.getResponseCode();

  if (code !== 200) {
    throw new Error('Falha ao buscar conversoes. HTTP ' + code + ': ' + response.getContentText());
  }

  return JSON.parse(response.getContentText());
}

// ---------------------------------------------------------------------------
// Funcao auxiliar: converte timestamp ISO 8601 para o formato que o Google Ads
// espera: YYYY-MM-DD HH:MM:SS
// Exemplo: "2026-05-18T14:30:00.000Z" -> "2026-05-18 14:30:00"
// ---------------------------------------------------------------------------
function formatConversionTime(isoTimestamp) {
  if (!isoTimestamp) {
    return '';
  }

  // Remove o sufixo de timezone e milissegundos, substitui T por espaco
  var formatted = isoTimestamp.replace('T', ' ').replace(/\.\d+.*$/, '').replace('Z', '');

  // Garante que o formato esta correto (YYYY-MM-DD HH:MM:SS)
  var match = formatted.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})/);
  if (match) {
    return match[1] + ' ' + match[2];
  }

  return formatted;
}

// ---------------------------------------------------------------------------
// Funcao auxiliar: registra uma mensagem na aba de Log com timestamp
// ---------------------------------------------------------------------------
function registrarLog(mensagem) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var abaLog = ss.getSheetByName(ABA_LOG);

  if (!abaLog) {
    abaLog = ss.insertSheet(ABA_LOG);
    abaLog.getRange('A1').setValue('Timestamp');
    abaLog.getRange('B1').setValue('Mensagem');
    abaLog.getRange('A1:B1').setFontWeight('bold');
  }

  // Adiciona nova linha no final da aba de Log
  var ultimaLinha = abaLog.getLastRow() + 1;
  var agora = Utilities.formatDate(new Date(), 'America/Sao_Paulo', 'yyyy-MM-dd HH:mm:ss');
  abaLog.getRange(ultimaLinha, 1).setValue(agora);
  abaLog.getRange(ultimaLinha, 2).setValue(mensagem);
}

// ---------------------------------------------------------------------------
// Funcao auxiliar: escreve os dados em uma aba no formato do Google Ads
//
// Parametros:
//   nomeAba    - nome da aba na planilha
//   linhas     - array de arrays, cada sub-array e uma linha de dados
//                [gclid, conversionName, conversionTime, value, currency]
// ---------------------------------------------------------------------------
function escreverAba(nomeAba, linhas) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var aba = ss.getSheetByName(nomeAba);

  if (!aba) {
    aba = ss.insertSheet(nomeAba);
  }

  // Limpa todo o conteudo existente para evitar dados antigos
  aba.clearContents();

  // Linha 1: parametro de timezone exigido pelo Google Ads
  aba.getRange('A1').setValue('Parameters:TimeZone=America/Sao_Paulo');

  // Linha 2: cabecalhos das colunas
  var headers = [
    'Google Click ID',
    'Conversion Name',
    'Conversion Time',
    'Conversion Value',
    'Conversion Currency'
  ];
  aba.getRange(2, 1, 1, headers.length).setValues([headers]);
  aba.getRange(2, 1, 1, headers.length).setFontWeight('bold');

  // Linhas de dados a partir da linha 3
  if (linhas.length > 0) {
    aba.getRange(3, 1, linhas.length, linhas[0].length).setValues(linhas);
  }
}

// ---------------------------------------------------------------------------
// Funcao principal: sincroniza conversoes do Supabase para a planilha
//
// Fluxo:
//   1. Login no Supabase para obter JWT
//   2. Busca leads com gclid preenchido e status relevante
//   3. Junta tudo em UMA aba (Conversoes) — o campo "Conversion Name"
//      diferencia "Reservar horario" de "Consulta Realizada"
//   4. Registra log com contagem de registros
// ---------------------------------------------------------------------------
function syncConversions() {
  var token;
  var leads;

  // Passo 1: Login no Supabase
  try {
    token = getSupabaseToken();
  } catch (e) {
    registrarLog('ERRO no login Supabase: ' + e.message);
    return;
  }

  // Passo 2: Buscar leads
  try {
    leads = fetchConversions(token);
  } catch (e) {
    registrarLog('ERRO ao buscar conversoes: ' + e.message);
    return;
  }

  // Passo 3: Montar todas as linhas em um unico array
  var todasLinhas = [];
  var countAgendamentos = 0;
  var countConsultas = 0;

  for (var i = 0; i < leads.length; i++) {
    var lead = leads[i];

    // Linha de agendamento (todos os leads retornados)
    todasLinhas.push([
      lead.gclid,
      'Reservar horario',
      formatConversionTime(lead.updated_at),
      '0.00',
      'BRL'
    ]);
    countAgendamentos++;

    // Linha de consulta realizada (apenas compareceu, com valor)
    if (lead.status === 'compareceu') {
      var valor = lead.valor_consulta ? parseFloat(lead.valor_consulta).toFixed(2) : '0.00';
      todasLinhas.push([
        lead.gclid,
        'Consulta Realizada',
        formatConversionTime(lead.updated_at),
        valor,
        'BRL'
      ]);
      countConsultas++;
    }
  }

  // Passo 4: Escrever na aba unica
  try {
    escreverAba(ABA_CONVERSOES, todasLinhas);
  } catch (e) {
    registrarLog('ERRO ao escrever na planilha: ' + e.message);
    return;
  }

  // Passo 5: Registrar no log
  var msg = 'Sync OK — '
    + countAgendamentos + ' agendamentos, '
    + countConsultas + ' consultas realizadas, '
    + todasLinhas.length + ' linhas totais.';
  registrarLog(msg);
}

// ---------------------------------------------------------------------------
// Cria as 3 abas necessarias caso nao existam (Agendamentos, Consultas, Log)
// Util para rodar na primeira vez, antes de configurar o trigger
// ---------------------------------------------------------------------------
function createSheets() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var abasNecessarias = [ABA_CONVERSOES, ABA_LOG];

  for (var i = 0; i < abasNecessarias.length; i++) {
    var nome = abasNecessarias[i];
    var aba = ss.getSheetByName(nome);

    if (!aba) {
      aba = ss.insertSheet(nome);
    }

    // Formata headers de acordo com o tipo de aba
    if (nome === ABA_LOG) {
      aba.getRange('A1').setValue('Timestamp');
      aba.getRange('B1').setValue('Mensagem');
      aba.getRange('A1:B1').setFontWeight('bold');
      aba.setColumnWidth(1, 180);
      aba.setColumnWidth(2, 500);
    } else {
      // Abas de conversao: escreve o parametro de timezone e headers
      aba.getRange('A1').setValue('Parameters:TimeZone=America/Sao_Paulo');
      var headers = [
        'Google Click ID',
        'Conversion Name',
        'Conversion Time',
        'Conversion Value',
        'Conversion Currency'
      ];
      aba.getRange(2, 1, 1, headers.length).setValues([headers]);
      aba.getRange(2, 1, 1, headers.length).setFontWeight('bold');

      // Ajusta largura das colunas para facilitar leitura
      aba.setColumnWidth(1, 280);
      aba.setColumnWidth(2, 180);
      aba.setColumnWidth(3, 180);
      aba.setColumnWidth(4, 140);
      aba.setColumnWidth(5, 160);
    }
  }

  registrarLog('Abas criadas/verificadas com sucesso.');
}

// ---------------------------------------------------------------------------
// Configura um trigger diario para rodar syncConversions as 6h da manha
// no fuso horario de Brasilia (America/Sao_Paulo)
//
// ATENCAO: Execute esta funcao apenas uma vez. Se executar novamente,
// um segundo trigger sera criado (duplicando a execucao diaria).
// Para remover triggers antigos, va em Editar > Acionadores do projeto.
// ---------------------------------------------------------------------------
function setupTrigger() {
  // Remove triggers anteriores desta funcao para evitar duplicatas
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    if (triggers[i].getHandlerFunction() === 'syncConversions') {
      ScriptApp.deleteTrigger(triggers[i]);
    }
  }

  // Cria novo trigger diario as 6h (fuso de Brasilia)
  // O Google Apps Script usa o fuso do projeto, que deve estar
  // configurado como America/Sao_Paulo no appscript.json
  ScriptApp.newTrigger('syncConversions')
    .timeBased()
    .everyDays(1)
    .atHour(6)
    .create();

  registrarLog('Trigger diario configurado: syncConversions as 06:00 (Brasilia).');
}
