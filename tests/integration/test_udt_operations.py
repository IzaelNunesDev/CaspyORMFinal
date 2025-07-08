# tests/integration/test_udt_operations.py

import pytest
from datetime import datetime
from src.caspyorm.core.model import Model
from src.caspyorm.core.fields import Text, Integer, Boolean, Float, UserDefinedType, Tuple
from src.caspyorm.types.usertype import UserType
from src.caspyorm.core.connection import ConnectionManager


class Address(UserType):
    """UDT para endereço."""
    street: Text = Text()
    city: Text = Text()
    state: Text = Text()
    zip_code: Text = Text()


class Contact(UserType):
    """UDT para informações de contato."""
    phone: Text = Text()
    email: Text = Text()
    address: UserDefinedType = UserDefinedType(Address)


class Coordinates(UserType):
    """UDT para coordenadas geográficas."""
    latitude: Float = Float()
    longitude: Float = Float()


class Location(UserType):
    """UDT para localização com coordenadas."""
    name: Text = Text()
    coordinates: UserDefinedType = UserDefinedType(Coordinates)
    address: UserDefinedType = UserDefinedType(Address)


class User(Model):
    """Modelo de usuário com UDTs."""
    __table_name__ = "users_with_udts"
    
    id: Text = Text(primary_key=True)
    name: Text = Text()
    contact: UserDefinedType = UserDefinedType(Contact)
    locations: UserDefinedType = UserDefinedType(Location)
    preferences: Tuple = Tuple(Text(), Boolean(), Integer())


class Restaurant(Model):
    """Modelo de restaurante com UDTs aninhados."""
    __table_name__ = "restaurants_with_udts"
    
    id: Text = Text(primary_key=True)
    name: Text = Text()
    address: UserDefinedType = UserDefinedType(Address)
    rating: Float = Float()
    is_open: Boolean = Boolean()


@pytest.fixture(scope="module")
def connection_manager():
    """Fixture para gerenciador de conexão."""
    manager = ConnectionManager()
    try:
        manager.connect(keyspace="test_keyspace")
        
        # Registrar UDTs
        manager.register_udt(Address)
        manager.register_udt(Contact)
        manager.register_udt(Coordinates)
        manager.register_udt(Location)
        
        # Sincronizar UDTs
        manager.sync_udts()
        
        yield manager
    finally:
        manager.disconnect()


@pytest.fixture(autouse=True)
def setup_teardown(connection_manager):
    """Setup e teardown para cada teste."""
    # Setup: criar tabelas
    User.create_table()
    Restaurant.create_table()
    
    yield
    
    # Teardown: limpar dados
    try:
        connection_manager.execute(f"TRUNCATE {User.__table_name__}")
        connection_manager.execute(f"TRUNCATE {Restaurant.__table_name__}")
    except:
        pass


class TestUDTOperations:
    """Testes de integração para operações com UDTs."""
    
    def test_create_udt_and_insert(self, connection_manager):
        """Testa criação de UDT e inserção de dados."""
        # Criar endereço
        address = Address(
            street="123 Main St",
            city="New York",
            state="NY",
            zip_code="10001"
        )
        
        # Criar contato
        contact = Contact(
            phone="+1-555-123-4567",
            email="john@example.com",
            address=address
        )
        
        # Criar coordenadas
        coords = Coordinates(latitude=40.7128, longitude=-74.0060)
        
        # Criar localização
        location = Location(
            name="Home",
            coordinates=coords,
            address=address
        )
        
        # Criar usuário
        user = User(
            id="user1",
            name="John Doe",
            contact=contact,
            locations=location,
            preferences=("dark_mode", True, 5)
        )
        
        # Salvar
        user.save()
        
        # Verificar se foi salvo
        saved_user = User.objects.get(id="user1")
        assert saved_user.name == "John Doe"
        assert saved_user.contact.phone == "+1-555-123-4567"
        assert saved_user.contact.address.city == "New York"
        assert saved_user.locations.coordinates.latitude == 40.7128
        assert saved_user.preferences == ("dark_mode", True, 5)
    
    def test_insert_udt_from_dict(self, connection_manager):
        """Testa inserção de UDT a partir de dicionário."""
        # Dados como dicionário
        contact_data = {
            "phone": "+1-555-987-6543",
            "email": "jane@example.com",
            "address": {
                "street": "456 Oak Ave",
                "city": "Los Angeles",
                "state": "CA",
                "zip_code": "90210"
            }
        }
        
        user = User(
            id="user2",
            name="Jane Smith",
            contact=contact_data,
            locations={
                "name": "Work",
                "coordinates": {"latitude": 34.0522, "longitude": -118.2437},
                "address": contact_data["address"]
            },
            preferences=("light_mode", False, 3)
        )
        
        user.save()
        
        # Verificar
        saved_user = User.objects.get(id="user2")
        assert saved_user.contact.email == "jane@example.com"
        assert saved_user.locations.name == "Work"
        assert saved_user.locations.coordinates.longitude == -118.2437
    
    def test_query_udt_fields(self, connection_manager):
        """Testa consulta de campos UDT."""
        # Inserir dados de teste
        address1 = Address(street="111 First St", city="Chicago", state="IL", zip_code="60601")
        address2 = Address(street="222 Second St", city="Miami", state="FL", zip_code="33101")
        
        restaurant1 = Restaurant(
            id="rest1",
            name="Pizza Place",
            address=address1,
            rating=4.5,
            is_open=True
        )
        
        restaurant2 = Restaurant(
            id="rest2",
            name="Burger Joint",
            address=address2,
            rating=3.8,
            is_open=False
        )
        
        restaurant1.save()
        restaurant2.save()
        
        # Consultar por campo UDT
        chicago_restaurants = Restaurant.objects.filter(address__city="Chicago")
        assert len(chicago_restaurants) == 1
        assert chicago_restaurants[0].name == "Pizza Place"
        
        # Consultar por múltiplos campos
        open_restaurants = Restaurant.objects.filter(is_open=True)
        assert len(open_restaurants) == 1
        assert open_restaurants[0].name == "Pizza Place"
    
    def test_update_udt_fields(self, connection_manager):
        """Testa atualização de campos UDT."""
        # Criar usuário
        address = Address(street="Old St", city="Old City", state="OC", zip_code="12345")
        contact = Contact(phone="old-phone", email="old@email.com", address=address)
        
        user = User(
            id="user3",
            name="Update Test",
            contact=contact,
            locations={
                "name": "Old Location",
                "coordinates": {"latitude": 0.0, "longitude": 0.0},
                "address": address
            },
            preferences=("old_pref", False, 1)
        )
        
        user.save()
        
        # Atualizar campos UDT
        new_address = Address(street="New St", city="New City", state="NC", zip_code="54321")
        new_contact = Contact(phone="new-phone", email="new@email.com", address=new_address)
        
        user.contact = new_contact
        user.locations.address = new_address
        user.preferences = ("new_pref", True, 10)
        user.save()
        
        # Verificar atualização
        updated_user = User.objects.get(id="user3")
        assert updated_user.contact.phone == "new-phone"
        assert updated_user.contact.address.city == "New City"
        assert updated_user.locations.address.street == "New St"
        assert updated_user.preferences == ("new_pref", True, 10)
    
    def test_nested_udt_operations(self, connection_manager):
        """Testa operações com UDTs aninhados."""
        # Criar estrutura complexa
        base_address = Address(
            street="Complex St",
            city="Complex City",
            state="CC",
            zip_code="99999"
        )
        
        home_coords = Coordinates(latitude=25.7617, longitude=-80.1918)
        work_coords = Coordinates(latitude=25.7617, longitude=-80.1918)
        
        home_location = Location(
            name="Home Complex",
            coordinates=home_coords,
            address=base_address
        )
        
        work_location = Location(
            name="Work Complex",
            coordinates=work_coords,
            address=base_address
        )
        
        contact = Contact(
            phone="complex-phone",
            email="complex@email.com",
            address=base_address
        )
        
        user = User(
            id="complex_user",
            name="Complex User",
            contact=contact,
            locations=home_location,
            preferences=("complex_pref", True, 7)
        )
        
        user.save()
        
        # Verificar estrutura aninhada
        saved_user = User.objects.get(id="complex_user")
        assert saved_user.contact.address.street == "Complex St"
        assert saved_user.locations.coordinates.latitude == 25.7617
        assert saved_user.locations.name == "Home Complex"
    
    def test_udt_with_collections(self, connection_manager):
        """Testa UDTs com coleções (List, Set, Map)."""
        from src.caspyorm.core.fields import List, Set, Map
        
        # Criar UDT com coleções
        class UserProfile(UserType):
            interests: List = List(Text())
            tags: Set = Set(Text())
            metadata: Map = Map(Text(), Text())
        
        # Registrar UDT
        connection_manager.register_udt(UserProfile)
        connection_manager.sync_udts()
        
        # Criar modelo com UDT que contém coleções
        class ProfileUser(Model):
            __table_name__ = "profile_users"
            
            id: Text = Text(primary_key=True)
            name: Text = Text()
            profile: UserDefinedType = UserDefinedType(UserProfile)
        
        ProfileUser.create_table()
        
        # Criar perfil com coleções
        profile = UserProfile(
            interests=["python", "cassandra", "databases"],
            tags={"developer", "backend", "python"},
            metadata={"experience": "5 years", "level": "senior"}
        )
        
        user = ProfileUser(
            id="profile_user1",
            name="Profile User",
            profile=profile
        )
        
        user.save()
        
        # Verificar
        saved_user = ProfileUser.objects.get(id="profile_user1")
        assert "python" in saved_user.profile.interests
        assert "developer" in saved_user.profile.tags
        assert saved_user.profile.metadata["experience"] == "5 years"
        
        # Limpar
        connection_manager.execute("TRUNCATE profile_users")
    
    def test_udt_validation_errors(self, connection_manager):
        """Testa erros de validação em UDTs."""
        # Tentar criar UDT com dados inválidos
        with pytest.raises(Exception):  # Pode ser ValidationError ou TypeError
            invalid_contact = Contact(
                phone="valid-phone",
                email="valid@email.com",
                address="not a dict or Address"  # Inválido
            )
        
        # Tentar salvar usuário com UDT inválido
        with pytest.raises(Exception):
            user = User(
                id="invalid_user",
                name="Invalid User",
                contact="not a contact",  # Inválido
                locations="not a location",  # Inválido
                preferences=("valid", True, 5)
            )
            user.save()
    
    def test_udt_batch_operations(self, connection_manager):
        """Testa operações em batch com UDTs."""
        from src.caspyorm.types.batch import BatchQuery
        
        # Criar múltiplos usuários com UDTs
        users_data = []
        for i in range(3):
            address = Address(
                street=f"{i}00 Test St",
                city=f"City {i}",
                state=f"ST{i}",
                zip_code=f"{i}0000"
            )
            
            contact = Contact(
                phone=f"+1-555-{i:03d}-0000",
                email=f"user{i}@example.com",
                address=address
            )
            
            location = Location(
                name=f"Location {i}",
                coordinates=Coordinates(latitude=float(i), longitude=float(i)),
                address=address
            )
            
            users_data.append({
                "id": f"batch_user_{i}",
                "name": f"Batch User {i}",
                "contact": contact,
                "locations": location,
                "preferences": (f"pref_{i}", bool(i % 2), i)
            })
        
        # Inserir em batch
        with BatchQuery() as batch:
            for user_data in users_data:
                user = User(**user_data)
                user.save()
        
        # Verificar se todos foram inseridos
        for i in range(3):
            user = User.objects.get(id=f"batch_user_{i}")
            assert user.name == f"Batch User {i}"
            assert user.contact.phone == f"+1-555-{i:03d}-0000"
            assert user.locations.coordinates.latitude == float(i) 