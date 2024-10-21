import zipfile
import os
import shutil
import sqlite3
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import datetime

app = FastAPI()

def convert_string_to_datetime(date_string: str) -> float:
    return datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').timestamp() * 1000

def merge_databases(db1_path: str, db2_path: str, output_db: str):
    # Conectar ao primeiro banco de dados
    conn1 = sqlite3.connect(db1_path)
    cursor1 = conn1.cursor()

    # Conectar ao segundo banco de dados
    conn2 = sqlite3.connect(db2_path)
    cursor2 = conn2.cursor()

    # Criar o banco de dados de saída
    conn_output = sqlite3.connect(output_db)
    cursor_output = conn_output.cursor()

    # Obter os registros deletados de ambas as bases de dados
    cursor1.execute("SELECT id, tableName, deletedAt FROM deleted_records_logs")
    deleted_logs_db1 = cursor1.fetchall()

    cursor2.execute("SELECT id, tableName, deletedAt FROM deleted_records_logs")
    deleted_logs_db2 = cursor2.fetchall()

    # Combinar os logs de exclusão de ambas as bases
    deleted_records = {}
    for row in deleted_logs_db1 + deleted_logs_db2:
        row_id, table_name, deleted_at = row
        deleted_at_converted = convert_string_to_datetime(deleted_at)
        if (row_id, table_name) not in deleted_records:
            deleted_records[(row_id, table_name)] = deleted_at_converted
        else:
            # Comparar qual deletedAt é mais recente e manter o mais recente
            if deleted_at_converted and (deleted_records[(row_id, table_name)] is None or deleted_at_converted > deleted_records[(row_id, table_name)]):
                deleted_records[(row_id, table_name)] = deleted_at_converted


    # Obter as tabelas do primeiro banco de dados
    cursor1.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor1.fetchall()

    for (table_name,) in tables:
        # Verificar se a tabela existe no segundo banco de dados
        cursor2.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}";')
        if cursor2.fetchone():
            try:
                # Obter o índice correto da coluna 'updatedAt'
                cursor1.execute(f'PRAGMA table_info("{table_name}");')
                columns = cursor1.fetchall()
                updated_at_index = next(i for i, col in enumerate(columns) if col[1] == 'updatedAt')

                # Merge dos dados com base na coluna 'updatedAt' e 'id'
                cursor1.execute(f'SELECT * FROM "{table_name}"')
                rows1 = cursor1.fetchall()

                cursor2.execute(f'SELECT * FROM "{table_name}"')
                rows2 = cursor2.fetchall()

                rows_by_id = {}

                # Adicionar todos os registros da primeira base
                for row in rows1:
                    row_id = row[0]  # Considera a coluna 'id' como a primeira
                    updated_at_1 = row[updated_at_index]

                    if (row_id, table_name) not in deleted_records or \
                       (deleted_records[(row_id, table_name)] is not None and updated_at_1 > deleted_records[(row_id, table_name)]):
                        rows_by_id[row_id] = row

                # Verificar e substituir se o registro no segundo banco for mais recente
                for row in rows2:
                    row_id = row[0]  # Coluna 'id'
                    updated_at_2 = row[updated_at_index]

                    if (row_id, table_name) in deleted_records:
                        deleted_at = deleted_records[(row_id, table_name)]
                        if updated_at_2 > deleted_at:
                            rows_by_id[row_id] = row
                        elif row_id in rows_by_id and rows_by_id[row_id][updated_at_index] < deleted_at:
                            del rows_by_id[row_id]
                    else:
                        if row_id in rows_by_id:
                            updated_at_1 = rows_by_id[row_id][updated_at_index]
                            if (updated_at_2 or datetime.now()) > (updated_at_1 or datetime.now()):
                                rows_by_id[row_id] = row
                        else:
                            rows_by_id[row_id] = row

                # Criar a tabela no banco de dados de saída
                columns_def = ", ".join([f'{col[1]} {col[2]}' for col in columns])

                cursor_output.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_def});')

                # Inserir os dados mesclados no banco de dados de saída
                placeholders = ", ".join(["?" for _ in columns])
                cursor_output.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', rows_by_id.values())

            except StopIteration:
                # Se não houver coluna 'updatedAt', copiar a tabela inteira da primeira db
                cursor1.execute(f'SELECT * FROM "{table_name}"')
                rows = cursor1.fetchall()
                cursor1.execute(f'PRAGMA table_info("{table_name}");')
                columns = cursor1.fetchall()
                columns_def = ", ".join([f'{col[1]} {col[2]}' for col in columns])

                cursor_output.execute(f'CREATE TABLE "{table_name}" ({columns_def});')
                placeholders = ", ".join(["?" for _ in columns])
                cursor_output.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', rows)
        else:
            # Se a tabela não existir no segundo banco, copiar ela inteira
            cursor1.execute(f'SELECT * FROM "{table_name}"')
            rows = cursor1.fetchall()
            cursor1.execute(f'PRAGMA table_info("{table_name}");')
            columns = cursor1.fetchall()
            columns_def = ", ".join([f'{col[1]} {col[2]}' for col in columns])

            cursor_output.execute(f'CREATE TABLE "{table_name}" ({columns_def});')
            placeholders = ", ".join(["?" for _ in columns])
            cursor_output.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', rows)

    # Commitar e fechar as conexões
    conn_output.commit()
    conn1.close()
    conn2.close()
    conn_output.close()


# Função para fazer o merge dos arquivos zip
def merge_zip_files(zip1_path: str, zip2_path: str, output_zip: str):
    # Diretórios temporários para extrair os arquivos
    temp_dir1 = "temp_dir1"
    temp_dir2 = "temp_dir2"

    # Criar os diretórios temporários
    os.makedirs(temp_dir1, exist_ok=True)
    os.makedirs(temp_dir2, exist_ok=True)

    # Extrair os arquivos ZIP
    with zipfile.ZipFile(zip1_path, 'r') as zip_ref1:
        zip_ref1.extractall(temp_dir1)

    with zipfile.ZipFile(zip2_path, 'r') as zip_ref2:
        zip_ref2.extractall(temp_dir2)

    # Dicionário para armazenar o arquivo mais recente ou regras específicas
    latest_files = {}

    # Comparar os arquivos e preservar os mais recentes
    for dirpath, _, filenames in os.walk(temp_dir1):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            relative_path = os.path.relpath(file_path, temp_dir1)
            if relative_path.endswith(".db"):
                # Se for um arquivo .db, aplicar a regra de merge de bancos de dados
                latest_files[relative_path] = ("db", file_path)
            else:
                file_mtime = os.path.getmtime(file_path)
                latest_files[relative_path] = (file_path, file_mtime)

    for dirpath, _, filenames in os.walk(temp_dir2):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            relative_path = os.path.relpath(file_path, temp_dir2)
            if relative_path.endswith(".db"):
                if relative_path in latest_files:
                    db1_path = latest_files[relative_path][1]
                    db2_path = file_path
                    output_db = f"merged_{relative_path}"
                    merge_databases(db1_path, db2_path, output_db)
                    latest_files[relative_path] = (output_db, None)
                else:
                    latest_files[relative_path] = ("db", file_path)
            else:
                file_mtime = os.path.getmtime(file_path)
                if (relative_path not in latest_files) or (file_mtime > latest_files[relative_path][1]):
                    latest_files[relative_path] = (file_path, file_mtime)

    # Criar um novo arquivo ZIP com os arquivos mesclados
    with zipfile.ZipFile(output_zip, 'w') as merged_zip:
        for relative_path, (file_path, _) in latest_files.items():
            merged_zip.write(file_path, relative_path)

    # Limpar diretórios temporários
    shutil.rmtree(temp_dir1)
    shutil.rmtree(temp_dir2)


# Modelo para receber os caminhos dos arquivos via JSON
class ZipPaths(BaseModel):
    zip1_path: str
    zip2_path: str


@app.post("/merge-local-zips")
async def merge_local_zips(paths: ZipPaths):
    output_zip = "merged_output.zip"
    merge_zip_files(paths.zip1_path, paths.zip2_path, output_zip)
    return {"message": "Merge completed", "output_zip": output_zip}


@app.get("/")
async def root():
    return {"message": "API para merge de System"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)