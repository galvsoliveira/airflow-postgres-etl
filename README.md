# Airflow-Postgres ETL (Ubuntu/WSL2) <!-- omit in toc -->

Neste projeto, efetuo um ETL de arquivos csv para um banco de dados Postgres utilizando o Airflow. O projeto foi desenvolvido em Ubuntu/WSL2.

---

## Table of Contents <!-- omit in toc -->

- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configurando a Máquina](#configurando-a-máquina)
- [Rodando o Projeto](#rodando-o-projeto)

---

## Estrutura do Projeto

O projeto está estruturado da seguinte forma:

```txt
airflow-postgres-etl/
├── config-scripts/
│ ├── 01_config_ubuntu_env.sh
│ ├── 02_install_asdf_plugins.sh
│ ├── 03_install_proj_dep.sh
│ ├── 04_requirements_txt_to_poetry(optional).sh
│ ├── stuck_resolving_dependencies_fix.sh
│ └── unistall_env.sh
├── dags/
│ ├── __init__.py
│ ├── .airflowignore
│ ├── datahelper/
│ │ ├── __init__.py
│ │ └── postgres.py
│ └── postgres_etl/
│   ├── events_table.py
│   └── tracking_table.py
├── data/
│ ├── extracted/
│ └── data.zip
├── initial-script/
│ └── extract-zip.py
├── .gitignore
├── .pre-commit-config.yaml
├── poetry.lock
├── pyproject.toml
├── credentials.json
└── .tool-versions
```

## Configurando a Máquina

- Configure o WSL2 com o seguinte tutorial: <https://github.com/galvsoliveira/airflow-postgres-etl#configuring-wsl2-for-windows-users>
- Clone o repositório e entre na pasta do projeto usando o terminal do Ubuntu.
- Rode os seguintes comandos:
  - `bash config-scripts/01_config_ubuntu_env.sh`
    - Reinicie o terminal e rode: `source ~/.bashrc`.
  - `bash config-scripts/02_install_asdf_plugins.sh`
  - `bash config-scripts/03_install_proj_dep.sh`
- Abra o VSCode usando o comando `code .`.
- Renomeie o arquivo `credentials.json.example` para `credentials.json`. As credenciais do exemplo são as mesmas do arquivo `docker-compose.yml`, então não é necessário alterar nada.

## Rodando o Projeto

- No terminal, rode:
  - `cd local-run`, para entrar na pasta local-run.
  - `chmod +x ./local-run`, para dar permissão de execução ao script.
  - `./local-run build-and-start`, pára construir o container do airflow e do postgres e iniciar o projeto.

Para parar o projeto, rode `./local-run stop`.
Agora, para acessar o Airflow, abra o navegador e digite `localhost:8080`. O usuário e senha são `airflow`. Rode as DAGs `postgres_etl.events_table` e `postgres_etl.tracking_table` para executar o ETL.
