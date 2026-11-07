# Infinite Fight · Atualização "Cidade do Caos"

## Como colocar no ar
1. Envie para o servidor (Render) **`index.html`** e **`server.js`** desta pasta. `package.json` e `render.yaml` não mudaram.
2. Os originais estão em `originais/` (`index-14.html` e `server__1_.js`): guarde para poder voltar atrás.
3. **Não deixe `EV_DEBUG` ligado no servidor de verdade** (ele faz eventos acontecerem a cada ~20 s).
4. Os eventos já valem em produção. Para desligar todos: variável `EV_ON=0`. Para ligar de novo: apague a variável.

## Modo de teste (no seu computador)
`EV_DEBUG=1 node server.js` (Windows: `set EV_DEBUG=1&& node server.js`). Sorteio a cada ~15–20 s, chance 100%.
- `EV_ONLY=<evento>` força só um evento: `fragmentos`, `visitante`, `casa_impossivel`, `ruptura`.
- `EV_VIS=mercador|sentinela|duelista` escolhe o visitante.
- Exemplo: `EV_DEBUG=1 EV_ONLY=visitante EV_VIS=duelista node server.js`

## O que mudou
**Cidade (só no Chaos livre; o modo Equipes ficou igual):** 12 quarteirões (residencial, comercial, industrial, abandonado, praça), colisões, 3 construções com interior (casa, loja, galpão). Portas funcionam ao encostar (serve para celular). Quem está em outro lugar não é desenhado.

**Sistema de eventos (servidor decide tudo):** relógio que só anda com gente no Chaos · sorteio a cada 30–90 min · 3 categorias com peso/chance/descanso próprios · tempo mínimo de 15 min entre eventos · descanso por evento · incompatíveis nunca juntos · quem entra no meio recebe o estado atual.

| Evento | Categoria | Duração | Resumo |
|---|---|---|---|
| Chuva de Fragmentos | raro | 45 s | 16–20 impactos avisados, zonas perigosas, 4 💎 (+5 pts) |
| O Visitante | muito raro | 100 s | 3 visitantes (oferta / vigília / desafio), +15 a +40 pts |
| A Casa Impossível | muito raro | 180 s | 3 salas extras, armadilhas, 3 fragmentos e o baú (+100 ao 1º) |
| A Ruptura | lendário | 150 s | fenda, 15 criaturas e o Colosso, vida no servidor, prêmio por dano |

Frequência aproximada com a configuração atual, em tempo de sala ocupada (medida numa simulação de 3.000 h): raro ~1 a cada 4 h · muito raro ~1 a cada 25 h (dividido entre o Visitante e a Casa Impossível) · lendário ~1 a cada 70 h.
**Todos os números ficam no começo de cada bloco do `server.js`:** `EV_CFG` (raridade), `FR`, `VIS`, `CASA`, `RUP`.

## Arquivos
**Entrega:** `index.html`, `server.js`.
**Os mesmos blocos, separados para leitura** (o jogo não precisa deles): `cidade_etapa2.js`, `eventos_server.js`, `eventos_cliente.js`, `eventos_fragmentos_server.js`, `eventos_fragmentos_cliente.js`, `eventos_visitante_server.js`, `eventos_visitante_cliente.js`, `eventos_casa_server.js`, `eventos_casa_cliente.js`, `eventos_ruptura_server.js`, `eventos_ruptura_cliente.js`.
**Mudanças fora dos blocos novos:** `index.html`, uma palavra (`case 'ev':`); `server.js`, 4 linhas pequenas (envio do estado ao entrar, `events` no `/health`, e 4 mensagens novas).

## Despertar Secreto
Não existe no código. Só há ganchos vazios no servidor (`EV_HOOKS.legendStart` / `legendEnd`) e `ev.lastLegend`, que um evento lendário atualiza. Nada depende deles.

## Limitações conhecidas
- Não foi testado em navegador de verdade nem com jogadores reais; o equilíbrio (dano, vida do Colosso) é um palpite.
- O dano é calculado no aparelho de quem apanha (como no resto do jogo). O servidor não confere posições, só limites e tempos.
- Golpes e projéteis dos jogadores ainda atravessam paredes; só o movimento é bloqueado.
- Se o servidor reiniciar (comum no Render grátis), o relógio e os descansos dos eventos zeram.
- Se o mapa da cidade mudar, a lista `FR_PTS` (pontos de impacto) precisa ser gerada de novo.
- Minimapa e setas de jogadores ainda mostram quem está dentro de construções.
