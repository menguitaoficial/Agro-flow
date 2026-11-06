# Agro-flow
PoC – Orquestração com Apache Airflow (Agro / Irrigação)
Simulação de sensores (umidade do solo, umidade do ar, temperatura e previsão de chuva) que alimentam uma DAG que decide se aciona a irrigação.

Como rodar
docker compose up
Abra http://localhost:8080. Usuário admin; a senha é gerada no log do container (docker compose logs airflow | grep -i password) ou fica em standalone_admin_password.txt dentro do container.

Ative a DAG agro_irrigacao (roda a cada 2 min) ou use Trigger DAG w/ config.

Painel visual
O docker compose up também sobe um painel em http://localhost:8081 (nginx servindo dashboard/index.html). Ele lê data/historico_irrigacao.csv a cada 3 s e mostra os 4 sensores, a decisão atual (irrigando / aguardando chuva / solo adequado) e o histórico. Deixe o Airflow e o painel lado a lado na demo.

Roteiro de demo
Trigger com o parâmetro cenario:

cenário	resultado esperado
seco	ramo irrigar
chuvoso	ramo aguardar_chuva
umido	ramo nao_irrigar
auto	aleatório
O histórico das decisões fica em data/historico_irrigacao.csv.

Conceitos de Airflow mostrados
DAG e agendamento (schedule), catchup=False
TaskFlow API (@dag, @task) e passagem de dados via XCom
Paralelismo: 4 leituras de sensores independentes (fan-out) e consolidar (fan-in)
Branching (@task.branch) com regra de negócio
Trigger rules (none_failed_min_one_success) para juntar os ramos
Params na UI, retries e logs por task
Regra de decisão
Prob. de chuva ≥ 60% → aguardar chuva
Solo < 30%, ou solo < 40% com temperatura > 32 °C → irrigar (tempo proporcional ao déficit)
Caso contrário → não irrigar
Ideias de evolução
Trocar a simulação por MQTT/API de clima real (ex.: OpenWeather), S3/Postgres como destino, alertas por e-mail/Telegram, e um dashboard sobre o CSV.
