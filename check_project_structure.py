import os
import sys
from pathlib import Path

def check_structure():
    print("🛠️ Verificando estrutura do projeto...\n")

    root_path = Path(__file__).parent
    src_path = root_path / "src"
    influx_path = src_path / "influx"
    required_files = [
        src_path / "__init__.py",
        influx_path / "__init__.py",
        influx_path / "connection.py",
        src_path / "pipeline.py",
    ]

    if not src_path.exists():
        print("❌ Pasta 'src/' não encontrada.")
        return

    missing = [str(f.relative_to(root_path)) for f in required_files if not f.exists()]
    if missing:
        print("❌ Arquivos obrigatórios ausentes:")
        for m in missing:
            print("  -", m)
    else:
        print("✅ Todos os arquivos essenciais estão presentes.")

    # Testa importação
    try:
        sys.path.append(str(root_path))
        from src.influx.connection import get_influx_client
        print("✅ Importação do módulo 'src.influx.connection' bem-sucedida!")
    except Exception as e:
        print("❌ Erro ao importar 'get_influx_client':")
        print(e)

if __name__ == "__main__":
    check_structure()
