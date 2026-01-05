from airflow.hooks.base import BaseHook
from contextlib import closing
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        # Connection 정보 초기화 (커넥션 생성 없이 정보만 가져옴)
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self._conn_info = {
            'host': airflow_conn.host,
            'user': airflow_conn.login,
            'password': airflow_conn.password,
            'dbname': airflow_conn.schema,
            'port': airflow_conn.port
        }

    def get_conn(self):
        """psycopg2 커넥션 반환 (contextlib.closing과 함께 사용 권장)"""
        return psycopg2.connect(**self._conn_info)

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:                             
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue 
                
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self._conn_info["user"]}:{self._conn_info["password"]}@{self._conn_info["host"]}:{self._conn_info["port"]}/{self._conn_info["dbname"]}'
        engine = create_engine(uri)
        
        try:
            file_df.to_sql(name=table_name,
                                con=engine,
                                schema='public',
                                if_exists=if_exists,
                                index=False
                            )
        finally:
            # SQLAlchemy engine 명시적으로 dispose하여 커넥션 풀 해제
            engine.dispose()