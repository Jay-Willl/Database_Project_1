import pandas as pd
import os
import threading

work_dir = '/Users/blank/IdeaProjects/DB_project_1/src'
splits_dir = f'{work_dir}/splits'

df = pd.read_csv(f'{work_dir}/contract_info.csv')
if not os.path.exists(splits_dir):
    os.mkdir(splits_dir)


# center 1
def create_center():
    center = df.loc[1:50001, ['supply center', 'industry']]
    center.drop_duplicates(subset=['supply center', 'industry'], keep='first', inplace=True)
    file_name = f"{splits_dir}/center_sub.csv"
    center.to_csv(file_name, index=False)


# enterprise 2
def create_enterprise():
    enterprise = df.loc[1:50001, ['client enterprise', 'country', 'city', 'supply center', 'industry']]
    enterprise.drop_duplicates(subset=['client enterprise', 'country', 'city', 'supply center', 'industry'],
                               keep='first', inplace=True)
    file_name = f"{splits_dir}/enterprise_sub.csv"
    enterprise.to_csv(file_name, index=False)


# contract 3
def create_contract():
    contract = df.loc[1:50001, ['contract number', 'director', 'contract date', 'client enterprise']]
    contract.drop_duplicates(subset=['contract number', 'contract date'], keep='first', inplace=True)
    file_name = f"{splits_dir}/contract_sub.csv"
    contract.to_csv(file_name, index=False)


# salesman 4
def create_salesman():
    salesman = df.loc[1:50001,
               ['salesman number', 'salesman', 'gender', 'age', 'mobile phone', 'supply center', 'industry']]
    salesman.drop_duplicates(subset=['salesman number', 'salesman'], keep='first', inplace=True)
    file_name = f"{splits_dir}/salesman_sub.csv"
    salesman.to_csv(file_name, index=False)


# product 5
def create_product():
    product = df.loc[1:50001, ['product code', 'product name']]
    product.drop_duplicates(subset=['product code', 'product name'], keep='first', inplace=True)
    file_name = f"{splits_dir}/product_sub.csv"
    product.to_csv(file_name, index=False)


# orders 6
def create_orders():
    orders = df.loc[1:50001,
             ['product code', 'quantity', 'estimated delivery date', 'lodgement date', 'contract number',
              'salesman number']]
    orders.drop_duplicates(
        subset=['product code', 'quantity', 'estimated delivery date', 'lodgement date', 'contract number',
                'salesman number'], keep='first', inplace=True)
    file_name = f"{splits_dir}/orders_sub.csv"
    orders.to_csv(file_name, index=False)


# model 7
def create_model():
    model = df.loc[1:50001, ['product model', 'unit price', 'product code']]
    model.drop_duplicates(subset=['product model', 'unit price', 'product code'], keep='first', inplace=True)
    file_name = f"{splits_dir}/model_sub.csv"
    model.to_csv(file_name, index=False)

t1 = threading.Thread(target=create_center)
t2 = threading.Thread(target=create_enterprise)
t3 = threading.Thread(target=create_contract)
t4 = threading.Thread(target=create_salesman)
t5 = threading.Thread(target=create_product)
t6 = threading.Thread(target=create_orders)
t7 = threading.Thread(target=create_model)

t1.start()
t2.start()
t3.start()
t4.start()
t5.start()
t6.start()
t7.start()



