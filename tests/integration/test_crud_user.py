import pytest
from caspyorm.connection import connect_async, disconnect_async
from caspyorm.model import Model
from caspyorm.fields import Integer, Text

class User(Model):
    __table_name__ = "users_test"
    id = Integer(primary_key=True)
    name = Text()
    email = Text()

@pytest.mark.asyncio
async def test_crud_user():
    await connect_async(contact_points=["127.0.0.1"], keyspace="test_keyspace")
    await User.sync_table_async(auto_apply=True)

    # CREATE
    user = await User.create_async(id=1, name="Alice", email="alice@example.com")
    assert user.id == 1
    assert user.name == "Alice"
    assert user.email == "alice@example.com"

    # GET
    user2 = await User.get_async(id=1)
    assert user2 is not None
    assert user2.name == "Alice"

    # FILTER + ALL_ASYNC
    users = await User.filter(name="Alice").all_async()
    assert len(users) >= 1
    assert any(u.email == "alice@example.com" for u in users)

    # ASYNC FOR
    found = False
    async for u in User.filter(email="alice@example.com"):
        if u.name == "Alice":
            found = True
    assert found

    # UPDATE
    await user.update_async(name="Alicia")
    updated = await User.get_async(id=1)
    assert updated.name == "Alicia"

    # DELETE
    await user.delete_async()
    deleted = await User.get_async(id=1)
    assert deleted is None

    await disconnect_async() 