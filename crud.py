# Etapa 1 - Instalação e importação da biblioteca CouchDB - pip install couchdb
import couchdb
# Importar módulo socket
import socket
# Instalação e importação da biblioteca dotenv - pip install dotenc
from dotenv import load_dotenv
# Importação da biblioteca os (Vairáveis de ambiente do sistema operacional)
# Importação do módulo urllib.parse
import urllib.parse
# Instalando e importando a biblioteca - tabulate - pip install tabulate
from tabulate import tabulate
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Funcionalidade - Conexão com um banco CouchDB
def con():
    
    # Parâmetros de acesso - Usuário e senha
    user = os.getenv('USER_CDB')
    password = os.getenv('PASS_CDB')
    
    # Para uso dos parâmetros de conexão, alguns caracteres podem confundir o acesso, 
    # logo utiliza-se do módulo "urllib.parse.quote" para separar esta condição.
    
    safe_pass = urllib.parse.quote(password or "")
    
    # Parâmetros da url coucndb (http:<user><password>@lhost:<porta: 5984>)
    url = f'http://{user}:{safe_pass}@localhost:5984'
    
    # Atribuindo a conexão (co)
    co = couchdb.Server(url)
    
    # Atribuindo um banco (existente ou a ser criado)
    
    bank = 'p_cdb'
    
    # Condição da existência do banco na conexão
    if bank in co:
        # atribui-se o banco a variável
        db = co[bank]
        return db
    else:
        # Estabeleçe um tratamento para ausência do banco
        try:
            # Cria-se novo banco
            db = co.create(bank)
            return db
        # Exceção 1: # Socket: canal de comunicação entre o CouchDB e a rede. * gaierror: erro ao resolver o endereço (DNS/IP inválido).
        except socket.gaierror as e: 
            print(f'Falha ao conectar-se com o servidor. {e}')
        # Exceção 2: Verifica os parâmetros de conexão ao banco (usuário e senha)    
        except couchdb.Unauthorized as f:
            print(f'Falha de autenticação. {f}')
        # Verifica situação do servidor atual (status)
        except ConnectionRefusedError as g:
            print(f'Falha do servidor. {g}')        

# Funcionalidade - Listando documentos/registros do banco
def listar ():
    
    # Atribui-se a conexão
    db = con()
    
    # Atribuindo uma lista
    usuarios = []
    
    # Criando um loop para contabilizar as linhas do banco (row), utlizndo método - view
    if db:
        # Se não houver registros, então...
        if db.info()['doc_count'] == 0:
            print('\n>>> Não há usuários registrados <<<')
        else:    
            for row in db.view('all_docs', include_docs=True):
                # Atribui-se ao documento (doc) a lista obtida
                doc = row['doc']
                # Atribui-se uma lista para os campos da tabela
                tab = [
                    doc.get('_id'),
                    doc.get('_rev'),
                    doc.get('_nome'),
                    doc.get('_idade'),
                    doc.get('_cidade')
                ]
                # Adiciona-se os campos da tabela a lista de usuários
                usuarios.append(tab)
                # Cria-se um cabeçalho da tabela
                headers = ["ID", "REV", "NOME", "IDADE", "CIDADE"]
                return print(tabulate(usuarios, headers=headers, tablefmt="simple"))
    else:
        print('\n>>> Sem conexão com o servidor. <<<')             
        
        
    
    
    