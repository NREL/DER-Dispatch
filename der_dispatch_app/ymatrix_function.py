# Copyright (c) 2019 Alliance for Sustainable Energy, LLC
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import numpy as np
from scipy.sparse import lil_matrix
import scipy.sparse.linalg as sp
import scipy.sparse as sparse
import csv
from scipy.sparse import csr_matrix, vstack, hstack

import query_model_adms as query_model

def re_orgnaize_for_volt_dict(V1_temp,all_node_index,NewNodeNames):
    V1 = [complex(0, 0)] * len(V1_temp)
    count = 0
    for node in NewNodeNames:
        index = all_node_index[node]
        V1[index] = V1_temp[count]
        count = count + 1
    return V1

def get_base_voltages(AllNodeNames,fidselect):
    lines = query_model.get_line_segements(fidselect)
    switches = query_model.get_switches(fidselect)
    trans1 = query_model.get_transformer_with_tanks(fidselect)
    trans2 = query_model.get_transformer_no_tanks(fidselect)
    data = query_model.get_basev_from(lines, switches, trans1, trans2)
    Vbase_allnode = np.array([data[node] for node in AllNodeNames])
    # Vbase_allnode = []
    # for node in AllNodeNames:
    #     Vbase_allnode.append(data[node]) # * 1000?
    return Vbase_allnode

def construct_Ymatrix_amds(Ysparse, slack_no,totalnode_number):
    '''
    Read platform y maxtrix file. It has no '[', ']' or '='
    :param Ysparse:
    :param slack_no:
    :param totalnode_number:
    :return: Y00,Y01,Y10,Y11,Y11_sparse,Y11_inv
    '''
    # Ymatrix = np.matrix([[complex(0, 0)] * totalnode_number] * totalnode_number)
    Ymatrix = np.array([[complex(0, 0)] * totalnode_number] * totalnode_number)
    G = []
    B = []
    with open(Ysparse, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',',)
        next(reader, None)  # skip the headers
        for row in reader:
            # print ', '.join(row)
            G.append(float(row[2]))
            B.append(float(row[3]))
            row_value = int(row[0])
            column_value = int(row[1])
            Ymatrix[row_value - 1, column_value - 1] = complex(G[-1], B[-1])
            Ymatrix[column_value - 1, row_value - 1] = Ymatrix[row_value - 1, column_value - 1]

    Y00 = Ymatrix[0:slack_no, 0:slack_no]
    Y01 = Ymatrix[0:slack_no, slack_no:]
    Y10 = Ymatrix[slack_no:, 0:slack_no]
    Y11 = Ymatrix[slack_no:, slack_no:]
    Y11_sparse = lil_matrix(Y11)
    Y11_sparse = Y11_sparse.tocsr()
    a_sps = sparse.csc_matrix(Y11)
    lu_obj = sp.splu(a_sps)
    Y11_inv = lu_obj.solve(np.eye(totalnode_number-slack_no))
    return [Y00,Y01,Y10,Y11,Y11_sparse,Y11_inv]

def construct_Ymatrix_amds_slack_sparse(Ysparse, slack_no, slack_start, slack_end, totalnode_number):
    '''
    Read platform y maxtrix file. It has no '[', ']' or '='
    Handels slack nodes positions not at the top-left of the matrix
    :param Ysparse:
    :param slack_no:
    :param totalnode_number:
    :return: Y00,Y01,Y10,Y11,Y11_sparse,Y11_inv
    '''

    Ymatrix = sparse.lil_matrix((totalnode_number, totalnode_number), dtype=np.complex_)

    slack_end += 1
    with open(Ysparse, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', )
        next(reader, None)  # skip the headers
        for row in reader:
            row_value = int(row[0])
            column_value = int(row[1])
            r = row_value - 1
            c = column_value - 1
            g = float(row[2])
            b = float(row[3])
            Ymatrix[r, c] = complex(g, b)
            Ymatrix[c, r] = Ymatrix[r, c]


    Y00 = Ymatrix[slack_start:slack_end, slack_start:slack_end].toarray()
    # print(slack_start, slack_end)
    # print(Ymatrix[slack_start:slack_end, :slack_start])
    # print(Ymatrix[slack_start:slack_end, slack_end:])
    Y01 = hstack((Ymatrix[slack_start:slack_end, :slack_start], Ymatrix[slack_start:slack_end, slack_end:]))
    Y10 = vstack((Ymatrix[:slack_start, slack_start:slack_end], Ymatrix[slack_end:, slack_start:slack_end]))

    m1 = Ymatrix[:slack_start, :slack_start]
    m2 = Ymatrix[:slack_start, slack_end:]
    print(m1.shape, m2.shape)
    m_1_2 = sparse.hstack((m1, m2))
    # m_1_2 = lil_matrix(m_1_2)

    m1 = 0
    m2 = 0
    m3 = Ymatrix[slack_end:, :slack_start]
    m4 = Ymatrix[slack_end:, slack_end:]
    # m_3_4 = np.concatenate((m3.toarray(), m4.toarray()), axis=1)
    m_3_4 = hstack((m3, m4))
    # m_3_4 = lil_matrix(m_3_4)
    # m_3_4 = sparse.hstack(m3, m4)
    m3 = 0
    m4 = 0

    a_sps = vstack((m_1_2, m_3_4))

    lu_obj = sp.splu(a_sps.tocsc())
    Y11_inv = lu_obj.solve(np.eye(totalnode_number - slack_no))
    Y11_inv = sparse.csc_matrix(Y11_inv)
    return [Y00, Y01, Y10.tocsc(),  Y11_inv]



def construct_Ymatrix_amds_slack(Ysparse, slack_no, slack_start, slack_end, totalnode_number):
    '''
    Read platform y maxtrix file. It has no '[', ']' or '='
    Handels slack nodes positions not at the top-left of the matrix
    :param Ysparse:
    :param slack_no:
    :param totalnode_number:
    :return: Y00,Y01,Y10,Y11,Y11_sparse,Y11_inv
    '''
    # Ymatrix = np.matrix([[complex(0, 0)] * totalnode_number] * totalnode_number)
    Ymatrix = np.array([[complex(0, 0)] * totalnode_number] * totalnode_number)
    # tempY11 = np.array([[complex(0, 0)] * (totalnode_number - slack_no)] * (totalnode_number - slack_no))
    # tempY01 = np.array([[complex(0, 0)] * (totalnode_number - slack_no)] * slack_no)
    #     print Ymatrix.shape
    #     print tempY11.shape
    # G = []
    # B = []
    #     slack_start+=1
    slack_end += 1
    print(slack_start, slack_end)
    with open(Ysparse, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', )
        next(reader, None)  # skip the headers
        for row in reader:
            # print ', '.join(row)
            # G.append(float(row[2]))
            # B.append(float(row[3]))
            row_value = int(row[0])
            column_value = int(row[1])
            r = row_value - 1
            c = column_value - 1
            g = float(row[2])
            b = float(row[3])
            Ymatrix[r, c] = complex(g, b)
            Ymatrix[c, r] = Ymatrix[r, c]
            # r = row_value - 1
            # c = column_value - 1
            #             print(r,c)
            # if r < slack_start and c < slack_start:
            #     tempY11[r, c] = complex(G[-1], B[-1])
            #     tempY11[c, r] = tempY11[r, c]
            # elif r >= slack_end and c >= slack_end:
            #     #                 print((r,c), (r-slack_no,c-slack_no))
            #     tempY11[r - slack_no, c - slack_no] = complex(G[-1], B[-1])
            #     tempY11[c - slack_no, r - slack_no] = tempY11[r - slack_no, c - slack_no]

    #             if  r >= slack_start and r < slack_end:
    #                 print (r,c)
    #             if c < slack_start :
    #                 if  r > slack_start and r < slack_end:
    #                     print((r,c) )
    #                     tempY01[r,c]=complex(G[-1], B[-1])
    #                     tempY01[c,r]=tempY01[r,c]
    #             if c > slack_end:
    #                 if  r >= slack_start and r <= slack_end:
    #                     print((r,c), (r-slack_no,c-slack_no))
    #                     tempY01[r-slack_no,c-slack_no]=complex(G[-1], B[-1])
    #                     tempY01[c-slack_no,r-slack_no]=tempY01[r-slack_no,c-slack_no]

    Y00 = Ymatrix[slack_start:slack_end, slack_start:slack_end]
    Y01 = np.concatenate((Ymatrix[slack_start:slack_end, :slack_start], Ymatrix[slack_start:slack_end, slack_end:]),
                             axis=1)
    Y10 = np.concatenate((Ymatrix[:slack_start, slack_start:slack_end], Ymatrix[slack_end:, slack_start:slack_end]),
                             axis=0)

    m1 = Ymatrix[:slack_start, :slack_start]
    m2 = Ymatrix[:slack_start, slack_end:]
    print(m1.shape, m2.shape)
    m3 = Ymatrix[slack_end:, :slack_start]
    m4 = Ymatrix[slack_end:, slack_end:]
    m_1_2 = np.concatenate((m1, m2), axis=1)
    m_3_4 = np.concatenate((m3, m4), axis=1)
    Y11 = np.concatenate((m_1_2, m_3_4), axis=0)

    # Y00 = Ymatrix[0:slack_no, 0:slack_no]
    # Y01 = Ymatrix[0:slack_no, slack_no:]
    # Y10 = Ymatrix[slack_no:, 0:slack_no]
    # Y11 = Ymatrix[slack_no:, slack_no:]

    # print(np.allclose(Y00, tempY00))
    # print(np.allclose(Y01, tempY01))
    # print(np.allclose(Y10, tempY10))
    # print(np.allclose(Y11, tempY11))

    # Y00 = tempY00
    # Y01 = tempY01
    # Y10 = tempY10
    # Y11 = tempY11

    # Y11_sparse = lil_matrix(Y11)
    # Y11_sparse = Y11_sparse.tocsr()
    a_sps = sparse.csc_matrix(Y11)
    lu_obj = sp.splu(a_sps)
    Y11_inv = lu_obj.solve(np.eye(totalnode_number - slack_no))
    # eye = sparse.eye(totalnode_number - slack_no, format='csc')
    # Y11_inv = lu_obj.solve(eye)
    #     return [Y00,Y01,Y10,Y11,Y11_sparse,Y11_inv,Ymatrix, tempY01, tempY11]
    return [Y00, Y01, Y10,  Y11_inv]

def linear_powerflow_model_slack_sparse_1(Y00,Y01,Y10,Y11_inv,V1,slack_no, slack_start, slack_end):
    # voltage linearlization
    # V1_conj = np.conj(V1[slack_no:])
    V1_conj = np.conj(np.concatenate((V1[:slack_start],V1[slack_end+1:])))
    V1_conj_inv = 1 / V1_conj
    coeff_V = Y11_inv.toarray() * V1_conj_inv
    # coeff_V_P = coeff_V
    # coeff_V_Q = -1j*coeff_V
    coeff_V_P = []
    coeff_V_Q = []
    coeff_Vm = -np.dot(Y11_inv.toarray(),np.dot(Y10.toarray(),V1[slack_start:slack_end+1]))

    # voltage magnitude linearization
    m = coeff_Vm
    m_inv = 1 / coeff_Vm
    coeff_Vmag_k = abs(m)
    A = (np.multiply(coeff_V.transpose(),m_inv)).transpose()
    coeff_Vmag_Q = (np.multiply((-1j*A).real.transpose(),coeff_Vmag_k)).transpose()
    coeff_Vmag_k = 0
    coeff_Vmag_P = (np.multiply(A.real.transpose(),coeff_Vmag_k)).transpose()

    return [coeff_V_P, coeff_V_Q, coeff_Vm, coeff_Vmag_P, coeff_Vmag_Q, coeff_Vmag_k]