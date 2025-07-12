import pytest
import subprocess
import os
import time
import json
from caspyorm.core.connection import connect, disconnect
from caspyorm.utils.exceptions import ConnectionError

@pytest.fixture(scope="session", autouse=True)
def cassandra_service():
    """
    Fixture do Pytest para gerenciar o serviço Cassandra via Docker Compose.

    - Inicia o serviço antes da sessão de testes e espera até que esteja saudável.
    - Garante que o serviço seja derrubado após a conclusão dos testes.
    """
    docker_compose_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    
    # Comando para iniciar o serviço e esperar pela prontidão
    start_command = ["docker-compose", "-f", os.path.join(docker_compose_path, 'docker-compose.yml'), "up", "-d", "--wait"]
    
    # Comando para derrubar o serviço
    stop_command = ["docker-compose", "-f", os.path.join(docker_compose_path, 'docker-compose.yml'), "down"]

    try:
        print("\nIniciando serviço Cassandra e aguardando prontidão...")
        # Inicia o serviço
        subprocess.run(start_command, check=True, capture_output=True, text=True)
        print("Serviço Cassandra está pronto.")
        
        # Permite que os testes sejam executados
        yield
        
    finally:
        print("\nDerrubando serviço Cassandra...")
        # Garante que o serviço seja derrubado no final
        subprocess.run(stop_command, check=True, capture_output=True, text=True)
        print("Serviço Cassandra derrubado.")

@pytest.fixture(scope="session")
def db_connection(cassandra_service):
    """
    Fixture que estabelece e encerra a conexão com o banco de dados para a sessão de testes,
    com um mecanismo de nova tentativa e descobrimento de IP do contêiner para robustez.
    """
    max_retries = 10
    retry_delay = 3  # Aumentar o delay

    # Obter o IP do contêiner do Cassandra
    try:
        container_name = "cassandra_nyc"
        inspect_cmd = ["docker", "inspect", container_name]
        result = subprocess.run(inspect_cmd, check=True, capture_output=True, text=True)
        container_info = json.loads(result.stdout)
        ip_address = container_info[0]['NetworkSettings']['Networks']['cassandra_teste_default']['IPAddress']
        print(f"IP do contêiner Cassandra: {ip_address}")
    except (subprocess.CalledProcessError, KeyError, IndexError) as e:
        print(f"Falha ao obter o IP do contêiner: {e}")
        ip_address = "172.18.0.2" # Fallback para o IP do container padrão do docker-compose

    for attempt in range(max_retries):
        try:
            print(f"\nTentativa de conexão com o banco de dados ({attempt + 1}/{max_retries}) em {ip_address}...")
            connect(contact_points=[ip_address], keyspace="nyc_data", port=9042)
            print("Conexão com o banco de dados estabelecida com sucesso.")
            yield
            print("\nEncerrando conexão com o banco de dados...")
            disconnect()
            return
        except ConnectionError as e:
            if attempt < max_retries - 1:
                print(f"Falha na conexão: {e}. Tentando novamente em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"Erro final de conexão após {max_retries} tentativas.")
                raise
