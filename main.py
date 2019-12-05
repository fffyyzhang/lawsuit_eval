import logging
import argparse
import pandas as pd
import csv
import warnings
warnings.filterwarnings("ignore")


def process_one_judge_item(df, doc_id, j_no):

    ret = []

    # loop for all payment ralations in one judge_item
    for (amt, type), relation in df.groupby(['amt', 'type']):
        if relation.shape[0] > 2:
            logging.error('{0}:{1}同金额/类型赔偿关系超过2个实体'.format(doc_id, j_no))

        #某些判决项中的赔偿关系会出现只有payer和payee的情况
        _payer = relation[relation['role']=='payer']['name'].values
        payer = '' if len(_payer)==0 else _payer[0]
        _payee = relation[relation['role']=='payee']['name'].values
        payee = '' if len(_payee) == 0 else _payee[0]

        d = {
            'doc_id' : str(relation['doc_id'].values[0]),
            'judgement_item': str(relation['judgement_item'].values[0]),
            #'relation_type':'赔偿关系',
            'payer':payer,
            'payee':payee,
            'amt':amt,
            'type':type
        }

        ret.append(d)

    return ret


def trans_binary_format(fname_in):
    '''
        将融安格式的解析/人工标注文件格式转为二元关系格式
    '''
    df = pd.read_csv(fname_in)
    df.rename(columns={"n_doc_id":"doc_id", "judgmentItemNo":"judgement_item"},
              inplace=True)
    l_items = []

    for (doc_id, j_no), j_item in df.groupby(['doc_id', 'judgement_item']):
        #处理判决项中赔偿关系
        df_pay = j_item[j_item['role'].isin(['payer', 'payee'])]
        l_items.extend(process_one_judge_item(df_pay, doc_id, j_no))

        #处理判决项中的费用关系
        df_fy = j_item[j_item['role']=='fyPayer']

        if not df_fy.empty:
            #df_fy['relation_type'] = '费用关系'
            df_fy['fyPayer'] = df_fy['name']
            df_fy.drop(['role', 'name'], axis=1, inplace=True)
            d_tmp = list(df_fy.T.to_dict().values())
            l_items.extend(d_tmp)

    df_ret = pd.DataFrame(l_items)
    col_reorder = ['doc_id', 'judgement_item', 'payer', 'payee',
                   'amt', 'type', 'fyPayer', 'fyAmt', 'fyType', 'fyShare']
    df_ret = df_ret[col_reorder]
    df_ret.sort_values(by=['doc_id', 'judgement_item'], ascending=False, inplace=True)
    return df_ret



def preprocess(answer, input_file, flag_trans, skip_no_parse):
    df_labeled = trans_binary_format('data/labeled_data.csv')
    df_parse = trans_binary_format('data/parse_result.csv')

    if(skip_no_parse):
        doc_id_labeled = set(df_labeled['doc_id'])
        #由于解析结果同时解析了已标注/未标注的原始语料，此处过滤掉未标注的解析结果
        df_parse = df_parse[df_parse['doc_id'].isin(doc_id_labeled)]

    df_labeled.to_csv('data_transferred/all_answer_binary.csv', index=False)
    df_parse.to_csv('data_transferred/parse_result_binary.csv', index=False)

    d=1

# def mk_dict(infile):
#     with open(infile, encoding='utf-8') as fin:
#         reader = csv.reader(fin, delimiter=',')
#         for i, row in enumerate(reader):
#             if i==0:
#                 continue

def mk_dict(infile, ingnore_type = False):
    df = pd.read_csv(infile, index_col=['doc_id', 'judgement_item'])
    if ingnore_type:
        df.drop(['type', 'fyType'], axis=1, inplace=True)

    return df.T.to_dict()
    # df_pay = df[df['relation_type']=='赔偿关系']
    # df_pay = df_pay.reindex(['doc_id', 'judgement_item','payer', 'payee', 'amt'])
    #df_fy = df[df['relation_type']=='费用关系']




if __name__ == "__main__":
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--answer', help='标注文件')
    parser.add_argument('-i', '--input', help='解析结果文件')
    parser.add_argument('-t', '--transfer', type=bool, help='是否转为二元关系格式')
    #注：没解析出任何结果的文档原因是刑事案件没标注，在公网测试的时候跳过，融安内部测试时
    #此选项不能为True
    parser.add_argument('-sd', '--skip_doc_null_parse', type=bool,
                        help='跳过没解析出任何结果的文档')
    parser.add_argument('-it', '--ignore_type', type=bool, help='是否忽略赔偿/费用类型')
    args = parser.parse_args()
    flag_trans, input_file, answer = args.transfer, args.input, args.answer
    skip_no_parse = args.skip_doc_null_parse
    ingore_type = args.ignore_type

    preprocess(answer, input_file, flag_trans, skip_no_parse)
    #d = mk_dict('all_answer_binary.csv', skip_no_parse, ingore_type)


    d=1