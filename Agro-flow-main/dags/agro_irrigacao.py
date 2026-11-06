r"""
PoC: orquestração de irrigação inteligente com Apache Airflow.

Fluxo:
  [solo] [ar] [temperatura] [previsão de chuva]   <- leituras em paralelo
                     |
                 consolidar
                     |
                  decidir  (branch)
          /          |            \
      irrigar   aguardar_chuva   nao_irrigar
          \          |            /
                  registrar
"""
import csv
import os
import random
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

ARQUIVO_HISTORICO = "/opt/airflow/data/historico_irrigacao.csv"

# Limiares da regra de negócio
SOLO_CRITICO = 30        # % de umidade do solo abaixo do qual irriga sempre
SOLO_ATENCAO = 40        # % -> irriga se estiver muito quente
TEMP_ALTA = 32           # °C
CHUVA_PROVAVEL = 60      # % de probabilidade de chuva que adia a irrigação

# Faixas de cada sensor por cenário (para forçar situações na demo)
CENARIOS = {
    "seco":    dict(solo=(10, 28), ar=(20, 40), temp=(33, 40), chuva=(0, 20)),
    "chuvoso": dict(solo=(25, 45), ar=(70, 95), temp=(18, 26), chuva=(70, 100)),
    "umido":   dict(solo=(55, 80), ar=(55, 80), temp=(20, 28), chuva=(10, 40)),
    "auto":    dict(solo=(10, 80), ar=(20, 95), temp=(15, 40), chuva=(0, 100)),
}


def _leitura(sensor: str, params: dict, logical_date) -> float:
    """Simula um sensor: valor aleatório dentro da faixa do cenário escolhido."""
    faixa = CENARIOS[params["cenario"]][sensor]
    rng = random.Random(f"{sensor}-{logical_date.isoformat()}-{params['cenario']}")
    return round(rng.uniform(*faixa), 1)


@dag(
    dag_id="agro_irrigacao",
    description="Decide irrigação a partir de sensores de solo, ar, temperatura e previsão de chuva",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    tags=["agro", "poc", "irrigacao"],
    params={
        "cenario": Param(
            "auto",
            enum=list(CENARIOS),
            description="auto = valores aleatórios | seco | chuvoso | umido (para forçar cada caminho na demo)",
        ),
    },
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
)
def agro_irrigacao():

    # ---------- 1) Ingestão: 4 sensores lidos em paralelo ----------
    @task
    def ler_umidade_solo(params=None, logical_date=None):
        return _leitura("solo", params, logical_date)

    @task
    def ler_umidade_ar(params=None, logical_date=None):
        return _leitura("ar", params, logical_date)

    @task
    def ler_temperatura(params=None, logical_date=None):
        return _leitura("temp", params, logical_date)

    @task
    def ler_previsao_chuva(params=None, logical_date=None):
        return _leitura("chuva", params, logical_date)

    # ---------- 2) Consolidação ----------
    @task
    def consolidar(solo, ar, temp, chuva, logical_date=None):
        leitura = {
            "timestamp": logical_date.isoformat(timespec="seconds"),
            "umidade_solo": solo,
            "umidade_ar": ar,
            "temperatura": temp,
            "prob_chuva": chuva,
        }
        print(f"Leitura consolidada: {leitura}")
        return leitura

    # ---------- 3) Decisão (branch) ----------
    @task.branch
    def decidir(leitura):
        solo, temp, chuva = leitura["umidade_solo"], leitura["temperatura"], leitura["prob_chuva"]

        if chuva >= CHUVA_PROVAVEL and solo >= SOLO_CRITICO / 2:
            print(f"Chuva provável ({chuva}%): adiando irrigação")
            return "aguardar_chuva"
        if solo < SOLO_CRITICO or (solo < SOLO_ATENCAO and temp > TEMP_ALTA):
            print(f"Solo {solo}% / temp {temp}°C: irrigar")
            return "irrigar"
        print(f"Solo {solo}%: umidade adequada")
        return "nao_irrigar"

    # ---------- 4) Ações ----------
    @task
    def irrigar(leitura):
        # Quanto mais seco o solo e o ar, mais tempo de irrigação
        deficit = SOLO_ATENCAO - leitura["umidade_solo"]
        minutos = int(max(5, deficit * 1.5 + (30 - leitura["umidade_ar"]) * 0.3))
        print(f"IRRIGAÇÃO ACIONADA por {minutos} min")
        return {"acao": "irrigar", "minutos": minutos}

    @task
    def aguardar_chuva():
        print("Irrigação adiada: previsão de chuva")
        return {"acao": "aguardar_chuva", "minutos": 0}

    @task
    def nao_irrigar():
        print("Nenhuma ação necessária")
        return {"acao": "nao_irrigar", "minutos": 0}

    # ---------- 5) Registro (roda após qualquer ramo escolhido) ----------
    @task(trigger_rule="none_failed_min_one_success")
    def registrar(leitura, ti=None):
        resultado = next(
            r for r in ti.xcom_pull(task_ids=["irrigar", "aguardar_chuva", "nao_irrigar"]) if r
        )
        linha = {**leitura, **resultado}
        os.makedirs(os.path.dirname(ARQUIVO_HISTORICO), exist_ok=True)
        novo = not os.path.exists(ARQUIVO_HISTORICO)
        with open(ARQUIVO_HISTORICO, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=linha.keys())
            if novo:
                w.writeheader()
            w.writerow(linha)
        print(f"Registrado em {ARQUIVO_HISTORICO}: {linha}")

    # ---------- Encadeamento ----------
    leitura = consolidar(
        ler_umidade_solo(), ler_umidade_ar(), ler_temperatura(), ler_previsao_chuva()
    )
    ramo = decidir(leitura)

    t_irrigar, t_chuva, t_nao = irrigar(leitura), aguardar_chuva(), nao_irrigar()
    ramo >> [t_irrigar, t_chuva, t_nao]
    registrar(leitura) << [t_irrigar, t_chuva, t_nao]


agro_irrigacao()
