import boto3
import click
import logging
from os import path
from datetime import datetime, timedelta

s3 = boto3.resource('s3')
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

@click.command()
@click.option('--bucket', help='Nombre del Buckect', required=True)
@click.option('--init_path', help='Ruta inicial de busqueda', required=True)
@click.option('--year', help='Año en la ruta de busqueda', required=True)
@click.option('--month', help='Mes en la ruta de busqueda', required=True)
@click.option('--product', help='Producto del cual se desean eliminar los archivos', default="")
@click.option('--qty_months', help='Cantidad de meses a buscar hacia adelante', default="1")

def main(bucket, init_path, year, month, product, qty_months):
    result = delete_processed_files(bucket, init_path, int(year), int(month), product, int(qty_months))
    logging.info("Archivos eliminados: [%s]", result)


def delete_processed_files(bucket, init_path, year, month, product, qty_months):
    date = datetime(year, month, 1)
    i = 1
    deleted_files = 0

    while i <= qty_months:
        path_key = "{0}/{1}".format(init_path, date.strftime("%Y/%m/"))

        objs = get_objets(bucket, path_key, product)
        logging.info("Cantidad de archivos encontrados [%s] en ruta: [%s] bucket: [%s]",len(objs), path_key, bucket)

        for f in objs:
            result = delete_file(bucket, f)
            if result: deleted_files += 1

        date += timedelta(datetime.now().max.day)
        i += 1

    return deleted_files


def delete_file(bucket, key):
    try:
        s3.Object(bucket, key).delete()
        return True
    except Exception as e:
        logging.error("Error al eliminar el archivo [%s] del bucket [%s]. Error: %s", key, bucket, e)
        return False


def get_objets(bucket, prefix, product = ""):
    filter_objects = []
    try:
        objects = s3.Bucket(bucket).objects.filter(Prefix=prefix)
        for o in objects:
            object_name = path.split(o.key)[1]
            if object_name == "": continue
            if object_name.split("-")[3] == product or not product: filter_objects.append(o.key)

        return filter_objects
    except Exception as e:
        logging.error("Error al extraer los archivos del bucket  [%s] con el prefijo [%s]. Error: %s", bucket, prefix, e)
        return []


if __name__ == '__main__':
    logging.info("Iniciando programa...")
    main() # pylint: disable=E1123,E1120