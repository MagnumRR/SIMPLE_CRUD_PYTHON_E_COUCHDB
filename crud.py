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

# Funcionalidade - inserção de registros
def inserir ():
    
    # Atribui-se a conexão
    db = con()
    
    # Se existir conexão, então...
    if db:
        nome = input('Nome do usuário: ')
        idade = int(input('Idade: '))
        cidade = input('Cidade: ')

        # Cria-se uma lista com as entradas
        usuario = {'nome':nome, 'idade':idade, 'cidade':cidade}
        
        # Atribui-se ao banco a nova lista de registro
        us_db = db.save(usuario)
        
        # Conferência ao salvar
        if us_db:
            print(f'\n>>> O usuário [{nome}] foi registrado com sucesso! <<<')
        else:
            print('\n>>> Usuário não registrado. <<<')
    else:
        print('\n>>> Não foi possível conectar ao servidor. <<<')

# Funcionalidade - consulta unitária
def cons_doc (doc_id):
    
    # Recebe como parâmetro o id fornecido pelo usuário (doc_id)
    # Atribui-se a conexão
    db = con()
    
    # Atribui-se a variável o id
    doc = db.get(doc_id)
    
    # Conferindo a existência do id no banco
    if doc:
        row_doc = [[
            doc.get('_id'),
            doc.get('_rev'),
            doc.get('nome'),
            doc.get('idade'),
            doc.get('cidade')
        ]]
        # Cria-se um Lista com cabeçalho dos campos
        headers = ['ID', 'VER', 'NOME', 'IDADE', 'CIDADE']
        print(tabulate(row_doc, headers=headers, tablefmt='simple'))
    else:
        print('\n>>> Falha ao visulizar o registro <<<')        
                            
# Funcionalidade - Atualização de registros                            
def atualizar ():
    
    # Atribui-se a conexão
    db = con()
    
    # Se existir conexão, então...
    if db:
        doc_id = input('Informe o ID do usuário: ')
        # chama função de consulta ao usuário
        cons_doc(doc_id)
        doc = db.get(doc_id)
        
        # Tratamento do registro
        try:
            if input('>>> Atualizar esse registro (s- sim / n- não)?: ').lower() != 's':
                print('\n>>> Atualização cancelada. <<<')                
            else:
                # Recebe novo nome oou mantém o existente
                up_nome = input('Atualizar nome: ') or doc['nome']
                
                # Recebe nova idade ou mantém a idade existente
                t_idade = input('Atualizar idade: ')
                up_idade = int(t_idade) if t_idade else doc['idade']
                
                # Recebe nova cidade ou mantém a cidade existente
                up_cidade = input('Atualizar cidade: ') or doc['cidade']
            
                # Atribuindo a chave
                doc['nome'] = up_nome
                doc['idade'] = up_idade
                doc['cidade'] = up_cidade
                
                # Atualizando no banco
                db[doc_id] = doc
                print(f'\n>>> O usuário ({doc[doc_id]}) - {up_nome} foi atualizado com sucesso. <<<')
        except couchdb.http.ResourceNotFound as e:
            print(f'Registro não encontrado. {e}')
    else:
        print('\n>>> Não foi possível conctar ao servidor. <<<')                
                
# Funcionalidade - excluir registro
def excluir ():
    
    # Atribui-se a conexão
    db = con()
    
    # Se existir conexão, então...
    if db:
        # Recebe ID pelo usuário
        doc_id = input('Informe o ID do usuário: ')
        # chama função de consulta ao usuário
        cons_doc(doc_id)
        doc = db.get(doc_id)
        
        # Tratamento do registro
        try:
            if input('>>> Deseja excuir esse registro: (s- sim / n- não)?: ').lower() != 's':
                print('\n>>> Exclusão cancelada. <<<')                
            else:
                db.delete(doc)
                print('\n>>> Registro excluído com sucesso! <<<')
        except couchdb.http.ResourceNotFound as e:
            print(f'\n>>> Erro ao deletar o arquivo: {e} <<<')
    else:
        print('\n>>> Não foi possível conectar ao servidor <<<')     
        
    
        
        
    
    
    