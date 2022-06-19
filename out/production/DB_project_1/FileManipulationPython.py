import pandas as pd
import os
import time


# test case 1
def FileManipulation1():
    t1 = time.time()
    work_dir = '/Users/blank/IdeaProjects/DB_project_1/src/splits/orders_sub.csv'
    res_dir = '/Users/blank/IdeaProjects/DB_project_1/src/res'
    df = pd.read_csv(work_dir)
    if not os.path.exists(work_dir):
        print("no file found!")
        return
    if not os.path.exists(res_dir):
        os.mkdir(res_dir)

    df = df.loc[1:50001, ['product code', 'lodgement date']]
    temp = df.loc[:, 'lodgement date'].isnull()
    res = df.loc[temp]
    res = res.loc[:, 'product code']
    file_name = f"{res_dir}/case1.csv"
    res.to_csv(file_name, index=False)
    t2 = time.time()
    print(t2 - t1)


def FileManipulation2():
    t1 = time.time()
    work_dir = '/Users/blank/IdeaProjects/DB_project_1/src/splits/orders_sub.csv'
    res_dir = '/Users/blank/IdeaProjects/DB_project_1/src/res'
    df = pd.read_csv(work_dir)
    if not os.path.exists(work_dir):
        print("no file found!")
        return
    if not os.path.exists(res_dir):
        os.mkdir(res_dir)

    df = df.loc[1:50001, ['salesman number', 'quantity', 'lodgement date']]
    res1 = df.loc[(df['quantity'] == 1000), ['salesman number', 'quantity']]
    res2 = df.loc[(df['lodgement date'] < '2000-02-01'), ['salesman number', 'quantity']]
    res1.drop_duplicates(subset=['salesman number'], inplace=True)
    res2.drop_duplicates(subset=['salesman number'], inplace=True)
    print(res)
    file_name = f"{res_dir}/case2.csv"
    res.to_csv(file_name, index=False)
    t2 = time.time()
    print(t2 - t1)


if __name__ == '__main__':
    # FileManipulation1()
    FileManipulation2()
