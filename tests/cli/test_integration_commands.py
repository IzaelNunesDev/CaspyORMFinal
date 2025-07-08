"""
Testes de integração real para a CLI CaspyORM.
Usa o Cassandra real e dados reais para testar a funcionalidade completa.
"""

import pytest
import asyncio
from typer.testing import CliRunner
import sys
import os
import uuid
import re
from datetime import datetime


def clean_output(output):
    """Remove códigos de cor ANSI da saída."""
    # Remove códigos de cor ANSI
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', output)

# Adicionar o diretório raiz ao path para importar os módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from cli.main import app
from src.caspyorm import connection
from src.caspyorm.core.model import Model
from src.caspyorm.core.fields import UUID, Text, Integer, Boolean, Float, Timestamp


class TestRestaurant(Model):
    """Modelo para testar com dados reais dos restaurantes."""
    __table_name__ = "nyc_restaurants"
    
    id = UUID(primary_key=True)
    name = Text()
    cuisine = Text()
    borough = Text()
    address = Text()
    phone = Text()
    rating = Float()
    price_range = Text()
    latitude = Float()
    longitude = Float()
    created_at = Timestamp()
    updated_at = Timestamp()


class TestCLIIntegrationReal:
    """Testes de integração real usando Cassandra e dados reais."""
    
    @pytest.fixture
    def runner(self):
        """Fixture para o CliRunner."""
        return CliRunner()
    
    @pytest.fixture(scope="class")
    def setup_database(self):
        """Setup do banco de dados para os testes."""
        # Conectar ao Cassandra
        asyncio.run(connection.connect_async(
            contact_points=["127.0.0.1"],
            keyspace="caspyorm_app"
        ))
        
        yield
        
        # Cleanup
        asyncio.run(connection.disconnect_async())
    
    def test_connect_command_real(self, runner, setup_database):
        """Testa o comando de conexão com Cassandra real."""
        result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0
        assert "Conexão com Cassandra estabelecida" in result.output

    def test_migrate_status_real(self, runner, setup_database):
        """Testa o comando de status das migrações com dados reais."""
        result = runner.invoke(app, ["migrate", "status"])
        assert result.exit_code == 0
        assert "Status das Migrações" in result.output
        # Verifica se as migrações conhecidas estão listadas
        assert "V20250706040802__create_users_table.py" in result.output
        assert "V20250706072527__nyc_restaurants_data.py" in result.output

    def test_sql_command_select_all_restaurants(self, runner, setup_database):
        """Testa o comando SQL para selecionar todos os restaurantes."""
        result = runner.invoke(app, ["sql", "SELECT * FROM nyc_restaurants LIMIT 5"])
        assert result.exit_code == 0
        clean_result = clean_output(result.output)
        # Verifica se contém "Resultado" e "Query" (pode estar em linhas separadas)
        assert "Resultado" in clean_result and "Query" in clean_result
        assert "Total de registros:" in clean_result
        
        # Verifica se os dados dos restaurantes estão presentes
        assert any(name in clean_result for name in [
            "Katz's Delicatessen",
            "Joe's Pizza", 
            "Peter Luger Steak House",
            "Di Fara Pizza",
            "Arthur Avenue Retail Market",
            "Taverna Kyclades",
            "Randazzo's Clam Bar"
        ])

    def test_sql_command_count_restaurants(self, runner, setup_database):
        """Testa o comando SQL para contar restaurantes."""
        result = runner.invoke(app, ["sql", "SELECT COUNT(*) FROM nyc_restaurants"])
        assert result.exit_code == 0
        clean_result = clean_output(result.output)
        # Verifica se contém "Resultado" e "Query" (pode estar em linhas separadas)
        assert "Resultado" in clean_result and "Query" in clean_result
        # Deve ter 7 restaurantes inseridos
        assert "7" in clean_result

    def test_sql_command_filter_by_borough(self, runner, setup_database):
        """Testa o comando SQL com filtro por borough."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT name, cuisine, borough FROM nyc_restaurants WHERE borough = 'Manhattan'"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes de Manhattan
        output = result.output
        assert "Manhattan" in output
        assert any(name in output for name in ["Katz's Delicatessen", "Joe's Pizza"])

    def test_sql_command_filter_by_cuisine(self, runner, setup_database):
        """Testa o comando SQL com filtro por tipo de culinária."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT name, cuisine FROM nyc_restaurants WHERE cuisine = 'Pizza'"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes de pizza
        output = result.output
        assert "Pizza" in output
        assert any(name in output for name in ["Joe's Pizza", "Di Fara Pizza"])

    def test_sql_command_filter_by_rating(self, runner, setup_database):
        """Testa o comando SQL com filtro por rating."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT name, rating FROM nyc_restaurants WHERE rating >= 4.5 ORDER BY rating DESC"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes com rating alto
        output = result.output
        assert "4.7" in output  # Peter Luger
        assert "4.6" in output  # Di Fara
        assert "4.5" in output  # Katz's e Taverna Kyclades

    def test_sql_command_filter_by_price_range(self, runner, setup_database):
        """Testa o comando SQL com filtro por faixa de preço."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT name, price_range FROM nyc_restaurants WHERE price_range = '$$'"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes com preço médio
        output = result.output
        assert "$$" in output

    def test_sql_command_insert_new_restaurant(self, runner, setup_database):
        """Testa o comando SQL para inserir um novo restaurante."""
        # Gera um UUID único para o teste
        test_id = str(uuid.uuid4())
        test_name = f"Test Restaurant {test_id[:8]}"
        
        insert_query = f"""
        INSERT INTO nyc_restaurants (
            id, name, cuisine, borough, address, phone, rating, 
            price_range, latitude, longitude, created_at, updated_at
        ) VALUES (
            {test_id}, '{test_name}', 'Test Cuisine', 'Test Borough', 
            '123 Test St', '(555) 123-4567', 4.0, '$$', 40.7589, -73.9851, 
            '{datetime.now().isoformat()}', '{datetime.now().isoformat()}'
        )
        """
        
        result = runner.invoke(app, ["sql", insert_query])
        assert result.exit_code == 0
        assert "Query executada com sucesso" in result.output
        
        # Verifica se o restaurante foi inserido
        select_result = runner.invoke(app, [
            "sql", 
            f"SELECT name FROM nyc_restaurants WHERE id = {test_id}"
        ])
        assert select_result.exit_code == 0
        assert test_name in select_result.output
        
        # Cleanup - remove o restaurante de teste
        runner.invoke(app, [
            "sql", 
            f"DELETE FROM nyc_restaurants WHERE id = {test_id}"
        ])

    def test_sql_command_update_restaurant(self, runner, setup_database):
        """Testa o comando SQL para atualizar um restaurante."""
        # Primeiro, pega um restaurante existente
        select_result = runner.invoke(app, [
            "sql", 
            "SELECT id, name FROM nyc_restaurants LIMIT 1"
        ])
        assert select_result.exit_code == 0
        
        # Extrai o ID do primeiro restaurante (simplificado)
        # Em um teste real, você parsearia a saída para pegar o ID
        # Por simplicidade, vamos usar um update genérico
        update_query = """
        UPDATE nyc_restaurants 
        SET rating = 5.0, updated_at = '2025-07-06 18:00:00'
        WHERE name = 'Joe''s Pizza'
        """
        
        result = runner.invoke(app, ["sql", update_query])
        assert result.exit_code == 0
        assert "Query executada com sucesso" in result.output

    def test_sql_command_complex_query(self, runner, setup_database):
        """Testa uma query SQL complexa com múltiplos filtros."""
        result = runner.invoke(app, [
            "sql", 
            """
            SELECT name, cuisine, borough, rating 
            FROM nyc_restaurants 
            WHERE rating >= 4.0 AND borough IN ('Manhattan', 'Brooklyn')
            ORDER BY rating DESC
            """
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes com rating alto de Manhattan e Brooklyn
        output = result.output
        assert "Manhattan" in output or "Brooklyn" in output

    def test_sql_command_aggregation(self, runner, setup_database):
        """Testa comandos SQL com agregações."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT borough, COUNT(*) as count, AVG(rating) as avg_rating FROM nyc_restaurants GROUP BY borough"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna estatísticas por borough
        output = result.output
        assert "borough" in output.lower()

    def test_sql_command_error_handling(self, runner, setup_database):
        """Testa o tratamento de erros em queries SQL inválidas."""
        result = runner.invoke(app, ["sql", "SELECT * FROM table_that_does_not_exist"])
        assert result.exit_code == 1
        assert "Erro ao executar query" in result.output

    def test_sql_command_syntax_error(self, runner, setup_database):
        """Testa o tratamento de erros de sintaxe SQL."""
        result = runner.invoke(app, ["sql", "SELECT * FROM nyc_restaurants WHERE invalid_column = 1"])
        assert result.exit_code == 1
        assert "Erro ao executar query" in result.output

    def test_migrate_init_real(self, runner, setup_database):
        """Testa a inicialização de migrações com banco real."""
        result = runner.invoke(app, ["migrate", "init"])
        assert result.exit_code == 0
        assert "Tabela de migrações verificada" in result.output

    def test_info_command_real(self, runner, setup_database):
        """Testa o comando de informações com configuração real."""
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0
        assert "CaspyORM CLI" in result.output
        assert "Configuração:" in result.output
        assert "caspyorm_app" in result.output  # keyspace padrão

    def test_models_command_real(self, runner, setup_database):
        """Testa o comando de modelos com descoberta real."""
        result = runner.invoke(app, ["models"])
        assert result.exit_code == 0
        # Pode retornar "Nenhum modelo" se não houver modelos no diretório atual
        # ou listar modelos se existirem

    def test_full_workflow_real(self, runner, setup_database):
        """Testa um fluxo completo real da CLI."""
        # 1. Testa conexão
        result = runner.invoke(app, ["connect"])
        assert result.exit_code == 0
        
        # 2. Testa status das migrações
        result = runner.invoke(app, ["migrate", "status"])
        assert result.exit_code == 0
        
        # 3. Testa query SQL
        result = runner.invoke(app, ["sql", "SELECT COUNT(*) FROM nyc_restaurants"])
        assert result.exit_code == 0
        
        # 4. Testa informações
        result = runner.invoke(app, ["info"])
        assert result.exit_code == 0

    def test_sql_command_with_different_keyspace(self, runner, setup_database):
        """Testa o comando SQL especificando um keyspace diferente."""
        result = runner.invoke(app, [
            "--keyspace", "caspyorm_app",
            "sql", 
            "SELECT COUNT(*) FROM nyc_restaurants"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output

    def test_sql_command_large_result_set(self, runner, setup_database):
        """Testa o comando SQL com um conjunto grande de resultados."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT * FROM nyc_restaurants"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        assert "Total de registros: 7" in result.output

    def test_sql_command_empty_result(self, runner, setup_database):
        """Testa o comando SQL com resultado vazio."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT * FROM nyc_restaurants WHERE name = 'Restaurant That Does Not Exist'"
        ])
        assert result.exit_code == 0
        assert "não retornou resultados" in result.output

    def test_sql_command_with_special_characters(self, runner, setup_database):
        """Testa o comando SQL com caracteres especiais nos dados."""
        result = runner.invoke(app, [
            "sql", 
            "SELECT name FROM nyc_restaurants WHERE name LIKE '%Pizza%'"
        ])
        assert result.exit_code == 0
        assert "Resultados da Query" in result.output
        # Verifica se retorna restaurantes com "Pizza" no nome
        output = result.output
        assert "Pizza" in output


class TestCLIPerformance:
    """Testes de performance da CLI com dados reais."""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture(scope="class")
    def setup_database(self):
        asyncio.run(connection.connect_async(
            contact_points=["127.0.0.1"],
            keyspace="caspyorm_app"
        ))
        yield
        asyncio.run(connection.disconnect_async())

    def test_sql_command_performance(self, runner, setup_database):
        """Testa a performance de queries SQL simples."""
        import time
        
        start_time = time.time()
        result = runner.invoke(app, ["sql", "SELECT COUNT(*) FROM nyc_restaurants"])
        end_time = time.time()
        
        assert result.exit_code == 0
        # A query deve executar em menos de 5 segundos
        assert (end_time - start_time) < 5.0

    def test_multiple_queries_performance(self, runner, setup_database):
        """Testa a performance de múltiplas queries sequenciais."""
        import time
        
        queries = [
            "SELECT COUNT(*) FROM nyc_restaurants",
            "SELECT name FROM nyc_restaurants WHERE borough = 'Manhattan'",
            "SELECT AVG(rating) FROM nyc_restaurants",
            "SELECT borough, COUNT(*) FROM nyc_restaurants GROUP BY borough"
        ]
        
        start_time = time.time()
        for query in queries:
            result = runner.invoke(app, ["sql", query])
            assert result.exit_code == 0
        end_time = time.time()
        
        # Todas as queries devem executar em menos de 10 segundos
        assert (end_time - start_time) < 10.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"]) 