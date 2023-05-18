from datetime import datetime
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert
from geopy.distance import geodesic
import ast
import logging
import sys

from models import DeviceData, AggregatedData, mysql_base, CHUNK_SIZE

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])


class EtlProcess:
    def __init__(self):
        while True:
            try:
                self.psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
                self.connection = self.psql_engine.connect()
                break
            except OperationalError:
                sleep(0.1)
        self.mysql_engine = create_engine(environ["MYSQL_CS"])

    def run_etl_process(self):
        logging.info('Waiting for the data generator...')
        sleep(20)
        logging.info('ETL Starting...')
        data = self.extract_data()
        logging.info('Extracted Data From Database!')
        logging.info('Running Transformation...')
        transformed_data = self.transform_data(data)
        logging.info('Transformed Data!')
        logging.info('Loading Data into Mysql...')
        self.load_data(transformed_data)
        logging.info('Loaded Data into Mysql!')

    def extract_data(self):
        session = sessionmaker(bind=self.psql_engine)
        postgres_session = session()
        data = postgres_session.query(DeviceData).all()
        return data

    def transform_data(self, data):
        max_temps = {}
        data_points = {}
        distance_movement = {}
        for row in data:
            device_id = row.device_id
            temperature = row.temperature
            location = row.location
            time = int(row.time)
            date_time = datetime.fromtimestamp(time)
            hour = date_time.replace(minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')

            # Maximum temperatures per device per hour
            if device_id not in max_temps:
                max_temps[device_id] = {}

            if hour not in max_temps[device_id]:
                max_temps[device_id][hour] = temperature
            else:
                max_temps[device_id][hour] = max(max_temps[device_id][hour], temperature)

            # Amount of data points aggregated per device per hour
            if device_id not in data_points:
                data_points[device_id] = {}

            if hour not in data_points[device_id]:
                data_points[device_id][hour] = 1
            else:
                data_points[device_id][hour] += 1

            # Total distance of device movement per device per hour
            if device_id not in distance_movement:
                distance_movement[device_id] = {}

            if hour not in distance_movement[device_id]:
                distance_movement[device_id][hour] = {'distance': 0, 'location': (None, None)}

            location = ast.literal_eval(location)

            prev_location = distance_movement[device_id][hour]['location']
            if isinstance(prev_location, int):
                prev_lat, prev_lon = None, None
            else:
                prev_lat, prev_lon = prev_location
            if prev_lat is not None and prev_lon is not None:
                curr_lat, curr_lon = location['latitude'], location['longitude']
                distance = geodesic((prev_lat, prev_lon), (curr_lat, curr_lon)).meters
                distance_movement[device_id][hour]['distance'] += distance

            # Update the previous latitude and longitude with the current location
            distance_movement[device_id][hour]['location'] = (location['latitude'], location['longitude'])
            distance_movement[device_id][hour]['distance'] = round(distance_movement[device_id][hour]['distance'], 2)
        return {'max_temps': max_temps, 'data_points': data_points, 'distance_movement': distance_movement}

    def load_data(self, data: dict):
        mysql_base.metadata.create_all(bind=self.mysql_engine)
        session = sessionmaker(bind=self.mysql_engine)
        mysql_session = session()
        bulk_data = []
        for device_id, hourly_data in data['max_temps'].items():
            for hour, max_temp in hourly_data.items():
                data_point_count = data['data_points'][device_id][hour]
                distance = data['distance_movement'][device_id][hour]['distance']
                row_data = {
                    'device_id': device_id,
                    'hour': hour,
                    'max_temperature': max_temp,
                    'data_points': data_point_count,
                    'total_distance': distance
                }
                bulk_data.append(row_data)

        # Perform the bulk insert
        print(len(bulk_data), 'We have this much data')
        for i in range(0, len(bulk_data), CHUNK_SIZE):
            chunk = bulk_data[i:i + CHUNK_SIZE]
            stmt = insert(AggregatedData).values(chunk)
            stmt = stmt.on_duplicate_key_update(
                max_temperature=stmt.inserted.max_temperature,
                data_points=stmt.inserted.data_points,
                total_distance=stmt.inserted.total_distance
            )
            mysql_session.execute(stmt)

        mysql_session.commit()
        mysql_session.close()


if __name__ == '__main__':
    etl = EtlProcess()
    etl.run_etl_process()
