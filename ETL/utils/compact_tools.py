import patoolib
import logging
from zipfile import ZipFile
from pathlib import Path

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)


def unzip_file(file, extension=None):
    file = Path(file)
    folder = file.parent
    zip_obj = ZipFile(file)
    try:
        for name in zip_obj.namelist():
            if ".." in name:
                warnn = 'Possivel tentativa de implantar malware'
                logger.warn(
                    f"Atenção: arquivo com '..' no nome dentro de arquivo zip {file.absolute()}. {warnn}")
                continue
            if extension and not name.lower().endswith(extension.lower()):
                logger.debug(f"Arquivo inválido dentro de {file}: {name}")
                continue
            zip_obj.extract(name, folder)
    except Exception as error:
        logger.error(f"Erro ao extrair conteúdos do arquivo {file}.")
        raise
    else:
        file.unlink()


def zip_files(files, folder_output, name_zip='files.zip'):
    with ZipFile(f'{folder_output}/{name_zip}', 'w') as zip:
            for file in files:
                zip.write(file)


def extract_file(file, output=None):
    file = Path(file)
    outdir = output or file.absolute().parents[0]
    patoolib.extract_archive(str(file), outdir=str(outdir))


def compact_files(files, output, name='files', extension='zip'):
    files_normalized = []
    for file in files:
        file = str(Path(file).absolute())
        files_normalized.append(file)
    if extension.startswith('.'):
        extension = extension[1:]
    name_file = Path(f'{name}.{extension}')
    outpath = str(Path(output).absolute() / name_file)
    patoolib.create_archive(outpath, files_normalized)
