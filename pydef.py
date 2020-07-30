#!/u01/python/360/bin/python3

def create_logger(log_id):
        import sys, os, logging,time
        logger = logging.getLogger(log_id)
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(time.strftime(f"{os.getenv('HOME')}/edw/src/log/{log_id}_%Y%m%d%H%M%S.log"))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter( logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s') )
        logger.addHandler(fh)

        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(sh)

        log = logging.getLogger(log_id)
        log.info(time.strftime(f"Log file name: {os.getenv('HOME')}/edw/src/log/{log_id}_%Y%m%d%H%M%S.log"))
        #log.info(f"Dat folder path: {get_os_var(log_id, 'EDW_DATFILES')}")

        return log

def convert_date_to_timestamp(date_string):
        from pytz import timezone
        import time
        from  datetime import datetime
        date = datetime.strptime(date_string, "%m/%d/%Y %H:%M:%S")
        date_pacific = timezone('America/Los_Angeles').localize(date)
        timestamp_micros = int(date_pacific.timestamp() * 1000000)

        return timestamp_micros


def get_os_var(log_id, param):
        import logging, subprocess, sys
        log = logging.getLogger(log_id)
        log.info(f"Retrieving {param} value from OS environment")
        CMD = 'echo $(source ./../env/ops_cron_env.sh; echo $%s)' % param
        p = subprocess.Popen(CMD, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
        param_val = p.stdout.readlines()[0].strip().decode("utf-8")
        if param_val == '':
                log.error(f"Unable to retrieve {param} from environment")
#               ops_update(ops_folder,ops_job,'ERROR')
                sys.exit(1)
        else:
                return param_val


def get_ops_param(log_id, ops_folder, ops_job, param):
        import logging, cx_Oracle, sys
        log = logging.getLogger(log_id)
        log.info(f"Retrieving {param} from OPS_MAPPING_PARMETER table")
        ops_conn = cx_Oracle.connect('edw_ops', 'edw_ops', 'phxedwdbldascan.internal.salesforce.com:1531/bidwdev1', encoding = 'UTF-8', nencoding = 'UTF-8')
        #ops_conn = cx_Oracle.connect(f'/@edw_ops', encoding="UTF-8", nencoding="UTF-8")
        ops_cur = ops_conn.cursor()
        stmt = "select param_val from ops_mapping_parameter where folder_nam = :1 and job_nam = :2 and param_nam = '$$'||:3"
        try:
                ops_cur.execute(stmt,[ops_folder, ops_job, param])
        except:
                log.info(f"stmt is {stmt}")
                log.exception(f"Failed to retrieve {param} from OPS_MAPPING_PARMETER table")
#               ops_update(ops_folder,ops_job,'ERROR')
                sys.exit(1)
        results = ops_cur.fetchall()
        ops_conn.close()
        return results[0][0]

def ops_update(log_id, folder_name, job_name, status):
        import logging, cx_Oracle, sys
        from datetime import datetime
        log = logging.getLogger(log_id)
        ops_conn = cx_Oracle.connect('edw_ops', 'edw_ops', 'phxedwdbldascan.internal.salesforce.com:1531/bidwdev1', encoding = 'UTF-8', nencoding = 'UTF-8')
        #ops_conn = cx_Oracle.connect(f'/@edw_ops', encoding="UTF-8", nencoding="UTF-8")
        ops_cur = ops_conn.cursor()
        curr_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ops_status_stmt = """select count(*),count(decode(etl_job_sts_cd,'RUNNING',etl_job_sts_cd)),nvl(max(decode(etl_job_sts_cd,'COMPLETED',ops_audit_etl_jobs_key)),0)
        from ops_audit_etl_jobs where ETL_JOB_FOLDR_NAM = :1 and ETL_JOB_NAM = :2 """
        if status == 'NEW':
                ops_stmt = """INSERT into ops_audit_etl_jobs
                (OPS_AUDIT_ETL_JOBS_KEY, ETL_JOB_FOLDR_NAM, ETL_JOB_NAM, ETL_JOB_GROUP_NAM, ETL_JOB_START_TS, ETL_JOB_STS_CD, ETL_JOB_EXTR_START_TS, ETL_JOB_EXTR_END_TS, ETL_JOB_EXTR_BUS_DT, EXTR_TABLE_NAM, EXTR_DT_COL_NAM, TRGT_TABLE_NAM, MAX_OPS_AUDIT_ETL_JOBS_KEY, CONN_NAM)
                SELECT OPS_AUDIT_ETL_JOBS_KEY_SEQ.NEXTVAL, ETL_JOB_FOLDR_NAM, ETL_JOB_NAM, ETL_JOB_GROUP_NAM, SYSDATE, 'RUNNING', nvl(ETL_JOB_EXTR_END_TS, '1-jan-1900'), SYSDATE, TRUNC(SYSDATE), EXTR_TABLE_NAM, EXTR_DT_COL_NAM, TRGT_TABLE_NAM, ops_audit_etl_jobs_key, CONN_NAM
                FROM ops_audit_etl_jobs
                WHERE ops_audit_etl_jobs_key =
                (select max(ops_audit_etl_jobs_key) from ops_audit_etl_jobs
                where ETL_JOB_FOLDR_NAM = :1 and ETL_JOB_NAM = :2 and etl_job_sts_cd ='COMPLETED')"""
        elif status == 'COMPLETED':
                ops_stmt = """update ops_audit_etl_jobs
                set ETL_JOB_STS_CD = 'COMPLETED', ETL_JOB_END_TS =  SYSDATE
                WHERE ops_audit_etl_jobs_key =
                (select ops_audit_etl_jobs_key from ops_audit_etl_jobs
                where ETL_JOB_FOLDR_NAM = :1 and ETL_JOB_NAM = :2 and etl_job_sts_cd ='RUNNING')"""
        elif status == 'ERROR':
                ops_stmt = """update ops_audit_etl_jobs
                                set ETL_JOB_STS_CD = 'ERROR', ETL_JOB_END_TS =  SYSDATE
                                WHERE ops_audit_etl_jobs_key =
                                (select ops_audit_etl_jobs_key from ops_audit_etl_jobs
                                where ETL_JOB_FOLDR_NAM = :1 and ETL_JOB_NAM = :2 and etl_job_sts_cd ='RUNNING')"""
        try:
                ops_cur.execute(ops_status_stmt, [folder_name, job_name])
                for row in ops_cur:
                        cnt_ops_records = row[0]
                        cnt_active_ops_records = row[1]
                        cnt_success_ops_records = row[2]
                if cnt_ops_records == 0:
                        log.info(f"Ops statement {ops_status_stmt}")
                        log.error(f"{folder_name}.{job_name} is no_in Ops table")
                        sys.exit(1)
                elif status == 'NEW' and cnt_active_ops_records > 0:
                        log.info(f"Ops statement {ops_status_stmt}")
                        log.error(f"Ops table has an entry with running status for {folder_name}.{job_name}")
                        sys.exit(1)
                elif cnt_active_ops_records > 1 and (status == 'COMPLETED' or status == 'ERROR'):
                        log.info(f"Ops statement {ops_status_stmt}")
                        log.error(f"Ops table has more than one entry with running status for {folder_name}.{job_name}")
                        sys.exit(1)
                log.info(f"Updating ops table with {status}")
                ops_cur.execute(ops_stmt, [folder_name, job_name])
                ops_conn.commit()
                ops_conn.close()
        except:
                log.exception("Failed to update Ops Entry")
                sys.exit(1)

def convert_tup_json(tup):
        import json
        user_dict = {}
        for a in tup:
                user_dict.setdefault("ids", []).append(a[0])
        user_json = json.dumps(user_dict)
        return user_json,user_dict

def curr_time():
        from datetime import datetime
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

