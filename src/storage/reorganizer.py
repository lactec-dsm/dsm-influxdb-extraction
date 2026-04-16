import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.storage.file_writer import save_to_csv, save_to_parquet

RAW_ROOT = Path("data/raw")
REORGANIZED_ROOT = RAW_ROOT / "reorganized"


class ReorganizationError(Exception):
    pass


def parse_raw_path(src_path: Path, raw_root: Path = RAW_ROOT) -> dict | None:
    """
    Analisa um caminho de arquivo em data/raw e extrai os metadados
    esperados para reorganização.

    Estrutura esperada:
        data/raw/<measurement>/<device>/<measurement_index>/<location_facility_id>/year=YYYY/month=MM/<file>
    """
    try:
        relative_path = src_path.relative_to(raw_root)
    except ValueError:
        return None

    parts = relative_path.parts
    if len(parts) < 6:
        return None

    measurement = parts[0]
    device = parts[1]
    measurement_index = parts[2]
    location_facility_id = parts[3]
    year_part = parts[4]
    month_part = parts[5]

    if not year_part.startswith("year=") or not month_part.startswith("month="):
        return None

    year = year_part.split("=", 1)[1]
    month = month_part.split("=", 1)[1]

    if not year.isdigit() or not month.isdigit():
        return None

    return {
        "measurement": measurement,
        "device": device,
        "measurement_index": measurement_index,
        "location_facility_id": location_facility_id,
        "year": year,
        "month": month,
    }


def build_reorganized_path(
    src_path: Path,
    metadata: dict,
    target_root: Path = REORGANIZED_ROOT,
) -> Path:
    """
    Monta o caminho de destino com a nova hierarquia:
        location_facility_id/year=YYYY/month=MM/device/measurement_index/measurement/<file>
    """
    return (
        target_root
        / metadata["location_facility_id"]
        / f"year={metadata['year']}"
        / f"month={metadata['month']}"
        / metadata["device"]
        / metadata["measurement_index"]
        / metadata["measurement"]
        / src_path.name
    )


def scan_raw_files(
    raw_root: Path = RAW_ROOT,
    target_root: Path = REORGANIZED_ROOT,
    locations: list[str] | None = None,
    years: list[str] | None = None,
) -> Iterable[Path]:
    """Retorna todos os arquivos válidos encontrados em data/raw, excetuando a pasta reorganizada."""
    if not raw_root.exists():
        return []

    resolved_target = target_root.resolve()
    for path in raw_root.rglob("*"):
        if not path.is_file():
            continue

        try:
            if resolved_target in path.resolve().parents or path.resolve() == resolved_target:
                continue
        except FileNotFoundError:
            # Pode acontecer com links quebrados, ignore-os.
            continue

        metadata = parse_raw_path(path, raw_root=raw_root)
        if metadata is None:
            continue

        if locations is not None and metadata["location_facility_id"] not in locations:
            continue

        if years is not None and metadata["year"] not in years:
            continue

        yield path


def load_and_save(src_path: Path, dst_path: Path) -> None:
    """Lê um arquivo CSV ou Parquet e salva no destino preservando o formato."""
    suffix = src_path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(src_path)
        save_to_csv(df, dst_path)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(src_path)
        save_to_parquet(df, dst_path)
    else:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)


def reorganize_file(
    src_path: Path,
    target_root: Path = REORGANIZED_ROOT,
    dry_run: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
) -> tuple[Path, str]:
    """
    Reorganiza um único arquivo para a nova hierarquia.

    Se dry_run for True, apenas retorna o destino sem gravar.
    """
    metadata = parse_raw_path(src_path)
    if metadata is None:
        raise ReorganizationError(f"Caminho não compatível com a estrutura raw: {src_path}")

    dst_path = build_reorganized_path(src_path, metadata, target_root=target_root)
    if dst_path.exists() and not overwrite:
        if verbose:
            print(f"[SKIP] arquivo já existe: {dst_path}")
        return dst_path, "skipped"

    if dry_run:
        if verbose:
            print(f"[DRY RUN] {src_path} -> {dst_path}")
        return dst_path, "dry_run"

    load_and_save(src_path, dst_path)
    if verbose:
        print(f"[COPY] {src_path} -> {dst_path}")
    return dst_path, "copied"


@dataclass
class ReorganizationStats:
    matched: int = 0
    copied: int = 0
    skipped: int = 0
    dry_run: int = 0


def reorganize_all(
    raw_root: Path = RAW_ROOT,
    target_root: Path = REORGANIZED_ROOT,
    dry_run: bool = False,
    overwrite: bool = False,
    locations: list[str] | None = None,
    years: list[str] | None = None,
    verbose: bool = False,
) -> tuple[list[Path], ReorganizationStats]:
    """Reorganiza todos os arquivos válidos encontrados em data/raw."""
    reorganized_paths: list[Path] = []
    stats = ReorganizationStats()

    for src_path in scan_raw_files(
        raw_root=raw_root,
        target_root=target_root,
        locations=locations,
        years=years,
    ):
        dst_path, action = reorganize_file(
            src_path,
            target_root=target_root,
            dry_run=dry_run,
            overwrite=overwrite,
            verbose=verbose,
        )
        stats.matched += 1
        if action == "copied":
            stats.copied += 1
        elif action == "skipped":
            stats.skipped += 1
        elif action == "dry_run":
            stats.dry_run += 1

        reorganized_paths.append(dst_path)

    return reorganized_paths, stats


def normalize_filter_values(values: list[str] | None) -> list[str] | None:
    if not values:
        return None

    normalized: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                normalized.append(part)

    return normalized or None


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Reorganiza arquivos de data/raw para data/raw/reorganized com nova hierarquia."
    )
    parser.add_argument(
        "--source-root",
        default=str(RAW_ROOT),
        help="Diretório de origem dos arquivos raw.",
    )
    parser.add_argument(
        "--target-root",
        default=str(REORGANIZED_ROOT),
        help="Diretório de destino reorganizado.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Listar os arquivos que seriam reorganizados sem gravar.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescrever arquivos existentes no destino.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mostrar cada ação de reorganização no console.",
    )
    parser.add_argument(
        "--location",
        action="append",
        help=(
            "Filtrar por location_facility_id. "
            "Pode ser usado várias vezes ou com valores separados por vírgula."
        ),
    )
    parser.add_argument(
        "--year",
        action="append",
        help=(
            "Filtrar por ano. "
            "Pode ser usado várias vezes ou com valores separados por vírgula."
        ),
    )

    args = parser.parse_args()

    source_root = Path(args.source_root)
    target_root = Path(args.target_root)
    locations = normalize_filter_values(args.location)
    years = normalize_filter_values(args.year)

    reorganized, stats = reorganize_all(
        raw_root=source_root,
        target_root=target_root,
        dry_run=args.dry_run,
        overwrite=args.overwrite,
        locations=locations,
        years=years,
        verbose=args.verbose,
    )

    if args.dry_run:
        print("[DRY RUN] arquivos que seriam reorganizados:")
        for path in reorganized:
            print(path)
        print(f"Total: {len(reorganized)}")
    else:
        print(f"Reorganizados: {stats.copied} arquivos copiados para {target_root}")
        if stats.skipped:
            print(f"Pulados: {stats.skipped} arquivos já existiam.")
        if stats.matched == 0:
            print("Nenhum arquivo encontrado com os filtros informados.")
        elif stats.copied == 0 and stats.skipped == 0:
            print("Nenhuma ação executada.")


if __name__ == "__main__":
    main()
