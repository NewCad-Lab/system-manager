import zipfile
import os
import shutil
import sqlite3
import uvicorn
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime
from pathlib import Path

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


def merge_folders(folder_path2: str,  folder_path1 = 'systems/luciane/main', merged_folder_path = 'systems/luciane/temp-folder'):
    # Criar a nova pasta para os arquivos mesclados
    os.makedirs(merged_folder_path, exist_ok=True)

    # Dicionário para armazenar o arquivo mais recente ou regras específicas
    latest_files = {}

    # Comparar os arquivos e preservar os mais recentes da primeira pasta
    for dirpath, _, filenames in os.walk(folder_path1):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            relative_path = os.path.relpath(file_path, folder_path1)
            if relative_path.endswith(".db"):
                # Se for um arquivo .db, aplicar a regra de merge de bancos de dados
                latest_files[relative_path] = ("db", file_path)
            else:
                file_mtime = os.path.getmtime(file_path)
                latest_files[relative_path] = (file_path, file_mtime)

    # Comparar os arquivos da segunda pasta
    for dirpath, _, filenames in os.walk(folder_path2):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            relative_path = os.path.relpath(file_path, folder_path2)
            if relative_path.endswith(".db"):
                if relative_path in latest_files:
                    db1_path = latest_files[relative_path][1]
                    db2_path = file_path
                    output_db = os.path.join(merged_folder_path, f"merged_{relative_path}")
                    merge_databases(db1_path, db2_path, output_db)  # Descomente esta linha para mesclar os bancos de dados
                    latest_files[relative_path] = (output_db, None)
                else:
                    latest_files[relative_path] = ("db", file_path)
            else:
                file_mtime = os.path.getmtime(file_path)
                if (relative_path not in latest_files) or (file_mtime > latest_files[relative_path][1]):
                    latest_files[relative_path] = (file_path, file_mtime)

    # Copiar os arquivos mesclados para a nova pasta
    for relative_path, (file_path, _) in latest_files.items():
        if file_path and os.path.isfile(file_path):  # Verifica se o caminho do arquivo é válido
            destination_path = os.path.join(merged_folder_path, relative_path)
            destination_dir = os.path.dirname(destination_path)
            # Criar o diretório de destino se não existir
            os.makedirs(destination_dir, exist_ok=True)
            shutil.copy2(file_path, destination_path)

    # Deletar a primeira pasta
    shutil.rmtree(folder_path1)

    #  Deletar a segunda pasta
    #shutil.rmtree(folder_path2)

    # Renomear a nova pasta mesclada para o nome da pasta deletada
    new_folder_name = os.path.basename(folder_path1)  # Obtém o nome da pasta original
    new_merged_folder_path = os.path.join(os.path.dirname(merged_folder_path), new_folder_name)
    os.rename(merged_folder_path, new_merged_folder_path)


    return new_merged_folder_path  # Retorna o caminho da nova pasta mesclada renomeada


# Modelo para receber os caminhos dos arquivos via JSON
class SystemPaths(BaseModel):
    system1_path: str

@app.post("/merge-local-systems")
async def merge_local_systems(paths: SystemPaths):
    merge_folders(paths.system1_path)
    return {"message": "Merge completed"}

@app.post("/upload-folder/")
async def upload_folder(file: UploadFile = File(...)):
    # Diretório onde o arquivo zip será salvo
    upload_dir = "systems/luciane"
    os.makedirs(upload_dir, exist_ok=True)

    # Caminho completo para salvar o arquivo zip
    zip_path = os.path.join(upload_dir, file.filename)

    # Salvar o arquivo zip no servidor
    with open(zip_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)

    # Diretório onde o conteúdo será extraído
    extraction_dir = os.path.join(upload_dir, file.filename[:-4])  # Remove '.zip' para criar o nome da pasta
    os.makedirs(extraction_dir, exist_ok=True)

    # Extraindo o conteúdo do arquivo zip
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extraction_dir)

    # Excluir o arquivo zip após a extração
    os.remove(zip_path)

    return {
        "filename": file.filename,
        "zip_path": zip_path,
        "extracted_folder": extraction_dir,
    }


def zip_folder(folder_path):
    zip_file_path = f"{folder_path}.zip"

    # Cria o arquivo ZIP
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, folder_path))

    return zip_file_path


@app.get("/download-main-zip/")
async def download_zip():
    folder_path = "systems/luciane/main"

    if not os.path.isdir(folder_path):
        raise HTTPException(status_code=404, detail="Directory not found")

    zip_file_path = zip_folder(folder_path)

    return FileResponse(zip_file_path, media_type='application/zip', filename=os.path.basename(zip_file_path))


# Se desejar, pode adicionar uma rota para deletar o arquivo ZIP após o download
@app.on_event("shutdown")
def remove_zip_file():
    zip_file_path = "systems/luciane/main.zip"  # Ajuste para o caminho correto do arquivo ZIP
    if os.path.exists(zip_file_path):
        os.remove(zip_file_path)


@app.get("/")
async def root():
    return {"message": "API para merge de System"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)