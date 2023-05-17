from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime

mysql_base = declarative_base()
psql_base = declarative_base()


class AggregatedData(mysql_base):
    __tablename__ = 'aggregated_data'

    device_id = Column(String(255), primary_key=True)
    hour = Column(DateTime, primary_key=True)
    max_temperature = Column(Integer)
    data_points = Column(Integer)
    total_distance = Column(Float)


# NOTE: This is only used in analytics.py can be used in main as well
class DeviceData(psql_base):
    __tablename__ = 'devices'
    device_id = Column(String(255), primary_key=True)
    temperature = Column(Integer)
    location = Column(String(255))
    time = Column(String(255))
