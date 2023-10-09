import oracledb
from flask import Flask, request, jsonify

import os
# from dotenv import load_dotenv

# load_dotenv()

def create_connection():
    # connection = oracledb.connect(user=os.getenv("ORCL_USER"), password=os.getenv("ORCL_PASS"), dsn=os.getenv("ORCL_CS"))
    connection = oracledb.connect(user="rm92928", password="061003", dsn="oracle.fiap.com.br/ORCL")
    return connection

def create_schema():
    cursor = connection.cursor()
    cursor.execute(f"""
                   begin
                    begin
                        execute immediate 'drop table usuario cascade constraints';
                        exception when others then
                            if sqlcode <> -942 then
                                raise;
                            end if;
                    end;

                    begin
                        execute immediate 'drop table banco cascade constraints';
                        exception when others then
                            if sqlcode <> -942 then
                                raise;
                            end if;
                    end;

                    execute immediate 'create table usuario (
                        id_usuario INTEGER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
                        nome VARCHAR(50) NOT NULL,
                        CONSTRAINT pk_id_usuario PRIMARY KEY(id_usuario)
                    )';

                    execute immediate 'create table banco (
                        id_banco INTEGER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
                        nome VARCHAR(50) NOT NULL,
                        saldo FLOAT,
                        id_usuario INTEGER,
                        CONSTRAINT pk_id_banco PRIMARY KEY(id_banco),
                        CONSTRAINT fk_id_usuario FOREIGN KEY(id_usuario) REFERENCES usuario(id_usuario)
                    )';

                    commit;
                end;
                   """)

app = Flask(__name__)

@app.route('/')
def index():
    return "API base CP5 - Devops"

# get all users
@app.route('/usuario', methods=['GET'])
def get_all_usuarios():
    cursor = connection.cursor()
    # verificando se existe um banco cadastrado para o usuário
    cursor.execute("SELECT * from banco")
    rows = cursor.fetchall()
    if len(rows) == 0:
        cursor.execute("SELECT * from usuario")
        r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
        return jsonify(r)
    cursor.execute("SELECT u.id_usuario, u.nome as usuario_nome, b.id_banco, b.nome as banco_nome, b.saldo FROM usuario u INNER JOIN banco b ON u.id_usuario = b.id_usuario")
    
    rows = cursor.fetchall()
    
    result = {}
    
    for row in rows:
        user_id, user_nome, banco_id, banco_nome, saldo = row
        
        if user_id not in result:
            result[user_id] = {
                "id_usuario": user_id,
                "nome": user_nome,
                "bancos": []
            }
        
        result[user_id]["bancos"].append({
            "id_banco": banco_id,
            "nome": banco_nome,
            "saldo": saldo
        })
    
    result_list = list(result.values())
    
    return jsonify(result_list)

# get user by id
@app.route('/usuario/<int:id>', methods=['GET'])
def get_usuario_by_id(id):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * from banco where id_usuario = {id}")
    rows = cursor.fetchall()
    if len(rows) == 0:
        cursor.execute(f"SELECT * from usuario where id_usuario = {id}")
        r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
        return jsonify(r)
    cursor.execute(f"SELECT u.id_usuario, u.nome as usuario_nome, b.id_banco, b.nome as banco_nome, b.saldo FROM usuario u INNER JOIN banco b ON u.id_usuario = b.id_usuario WHERE u.id_usuario = {id}")
    
    # Fetch all the rows from the query
    rows = cursor.fetchall()
    
    # Create a dictionary to store the result
    result = {}
    
    for row in rows:
        # Extract user and bank information from the row
        user_id, user_nome, banco_id, banco_nome, saldo = row
        
        # If the user ID is not in the result dictionary, create an entry for the user
        if user_id not in result:
            result[user_id] = {
                "id_usuario": user_id,
                "nome": user_nome,
                "bancos": []
            }
        
        # Add bank information to the user's "bancos" list
        result[user_id]["bancos"].append({
            "id_banco": banco_id,
            "nome": banco_nome,
            "saldo": saldo
        })
    
    # Convert the result dictionary to a list of user dictionaries
    result_list = list(result.values())
    
    return jsonify(result_list)

# create user
@app.route('/usuario', methods=['POST'])
def create_usuario():
    cursor = connection.cursor()
    cursor.execute(f"insert into usuario (nome) values ('{request.json['nome']}')")
    cursor.execute(f"select * from usuario where nome = '{request.json['nome']}'")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# update user
@app.route('/usuario/<int:id>', methods=['PUT'])
def update_usuario(id):
    cursor = connection.cursor()
    cursor.execute(f"update usuario set nome = '{request.json['nome']}' where id_usuario = {id}")
    cursor.execute(f"select * from usuario where id_usuario = {id}")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# delete user
@app.route('/usuario/<int:id>', methods=['DELETE'])
def delete_usuario(id):
    cursor = connection.cursor()
    cursor.execute(f"delete from banco where id_usuario = {id}")
    cursor.execute(f"delete from usuario where id_usuario = {id}")
    return jsonify({'message': 'User deleted'})

# get all banks
@app.route('/banco', methods=['GET'])
def get_all_bancos():
    cursor = connection.cursor()
    cursor.execute("select * from banco")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# get bank by id
@app.route('/banco/<int:id>', methods=['GET'])
def get_banco_by_id(id):
    cursor = connection.cursor()
    cursor.execute(f"select * from banco where id_banco = {id}")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# create bank
@app.route('/banco', methods=['POST'])
def create_banco():
    cursor = connection.cursor()
    cursor.execute(f"insert into banco (nome, saldo, id_usuario) values ('{request.json['nome']}', {request.json['saldo']}, {request.json['id_usuario']})")
    cursor.execute(f"select * from banco where nome = '{request.json['nome']}'")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# update bank
@app.route('/banco/<int:id>', methods=['PUT'])
def update_banco(id):
    cursor = connection.cursor()
    cursor.execute(f"update banco set nome = '{request.json['nome']}', saldo = {request.json['saldo']}, id_usuario = {request.json['id_usuario']} where id_banco = {id}")
    cursor.execute(f"select * from banco where id_banco = {id}")
    r = [dict((cursor.description[i][0], value)
              for i, value in enumerate(row)) for row in cursor.fetchall()]
    return jsonify(r)

# delete bank
@app.route('/banco/<int:id>', methods=['DELETE'])
def delete_banco(id):
    cursor = connection.cursor()
    cursor.execute(f"delete from banco where id_banco = {id}")
    return jsonify({'message': 'Bank deleted'})

if __name__ == '__main__':

    connection = create_connection()

    # Create a demo table
    create_schema()

    # Start a webserver
    # app.run(port=int(os.environ.get('PORT', '8080')))
    app.run(port=8080)