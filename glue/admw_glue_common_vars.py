COLUMN_NAME_OG_FILE_HASH = "admw_file_hash"
COLUMN_NAME_FILE_HASH = "pre_admw_file_hash"
COLUMN_NAME_PRELIM_HASH = "pre_admw_prelim_hash"
COLUMN_NAME_FINAL_HASH = "pre_admw_final_hash"
COLUMN_NAME_PRELIM_ORIGIN_FILE_NAME = "pre_admw_origin_file_name"
COLUMN_NAME_FINAL_ORIGIN_FILE_NAME = "admw_origin_file_name"


class ReconDefs:
    RECON_STATUS_PASSED = "PASSED"
    RECON_STATUS_FAILED = "FAILED"
    COLUMN_NAME_RECON_OUTCOME = "reconciliation_outcome"
    HASH_EXPR = "UPPER(SHA2(CONCAT({0}), 256))"
    RECON_2_NUM_PASSED = 100
    RECON_2_NUM_FAILED = 1000


RECON_S3_PATH = "{0}_STAGE{1}_{2}.{3}"
EMPTY_RECON_CSV = "reconciliation_outcome,count\nEXCLUDED,EXCLUDED\n"
