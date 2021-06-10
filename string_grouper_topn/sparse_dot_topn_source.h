/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Author: Zhe Sun, Ahmet Erdem
// April 20, 2017
// Modified by: Particular Miner
// April 14, 2021

#ifndef UTILS_CPPCLASS_H
#define UTILS_CPPCLASS_H


struct candidate {int index; double value;};

extern bool candidate_cmp(candidate c_i, candidate c_j);

extern void sparse_dot_topn_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		double Ax[],	//data of A
		int Bp[],
		int Bj[],
		double Bx[],	//data of B
		int ntop,
		double lower_bound,
		int Cp[],
		int Cj[],
		double Cx[]		//data of C
);

extern int sparse_dot_topn_extd_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		double Ax[],	//data of A
		int Bp[],
		int Bj[],
		double Bx[],	//data of B
		int ntop,
		double lower_bound,
		int Cp[],
		int Cj[],
		double Cx[], 	//data of C
		std::vector<int>* alt_Cj,
		std::vector<double>* alt_Cx,
		int nnz_max,
		int* n_minmax
);

extern int sparse_dot_only_nnz_source(
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		double Ax[], //data of A
		int Bp[],
		int Bj[],
		double Bx[], //data of B
		int ntop,
		double lower_bound
);

#endif //UTILS_CPPCLASS_H