import opt_function
import numpy as np
import zipfile
import io
import ymatrix_function
import time

def test_linear_powerflow_model_slack_sparse_1():
    print(time.asctime(time.localtime()))
    Ysparse_file = 'FeederInfo_test/base_no_ysparse_temp.csv'
    node_number = 9493
    slack_start = 9480
    slack_end = 9483-1
    slack_number = 3
    [Y00, Y01, Y10, Y11_inv] = \
        ymatrix_function.construct_Ymatrix_amds_slack_sparse(Ysparse_file, slack_number, slack_start,
                                                             slack_end, node_number)
    V1 = np.ones(node_number)
    print(time.asctime(time.localtime()))
    linear_PFmodel_coeff = opt_function.linear_powerflow_model_slack_sparse_1(Y00, Y01, Y10,
                                                                                   Y11_inv, V1,
                                                                                   slack_number, slack_start,
                                                                                   slack_end)
    print(time.asctime(time.localtime()))
if __name__ == '__main__':
    test_linear_powerflow_model_slack_sparse_1()