import numpy as np
from os.path import exists
import pandas as pd
import psycopg2 as pg

CARDS = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2']


def get_connection_str(config_file):
    '''read each line from config file into a list'''
    config = [line.rstrip('\n') for line in open(config_file)]
    '''save config entries as variables'''
    dbname, user, host, password = config[0], config[1], config[2], config[3]
    '''create database connection string from variables'''
    conn_str = f'dbname={dbname} user={user} host={host} password={password}'
    return conn_str


def save_data(query_file, title, config_file='config.txt'):
    '''connect to database and execute query'''
    conn_str = get_connection_str(config_file)
    conn = pg.connect(conn_str)
    cur = conn.cursor()
    with open(query_file) as file:
        query = file.read()
    cur.execute(query)
    cols = [desc[0] for desc in cur.description]
    '''transform query results into a pandas df'''
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    df.to_csv(f'data/{title}.csv', encoding='utf-8', index=False)
    cur.close()
    return


def hand_to_matrix_format(df):
    '''
    Convert as follows to enable displaying hands in a matrix:
        suited hands: AsKs to AK,
        offsuit hands: AsKh to KA,
        paired hands: 9h9c to 99
    '''
    card1_ls = []
    card2_ls = []
    for i, suit in enumerate(df.card1_suit):
        card1 = ''
        card2 = ''
        if suit:
            if suit != df.card2_suit.iloc[i]:
                card1 = str(df.card2.iloc[i])
                card2 = str(df.card1.iloc[i])
            else:
                card1 = str(df.card1.iloc[i])
                card2 = str(df.card2.iloc[i])
        card1_ls.append(card1)
        card2_ls.append(card2)
    return card1_ls, card2_ls


def clean_data(file):
    df = pd.read_csv(file)
    df['c1'], df['c2'] = hand_to_matrix_format(df)
    df.c1 = pd.Categorical(df.c1, ordered=True, categories=CARDS)
    df.c2 = pd.Categorical(df.c2, ordered=True, categories=CARDS)
    return df


def main():
    '''fetch and save the data if it doesnt already exist'''
    if not exists('data/p_cards.csv'):
        save_data('p_cards_query.txt', 'p_cards')

    df = clean_data('data/p_cards.csv')
    hand_matrix = df.groupby(['c1', 'c2']).id_hand.agg(['count']).unstack()
    print(hand_matrix, '\n')
    hand_matrix_percentage = hand_matrix / df.shape[0] * 100.0
    print(np.round(hand_matrix_percentage, decimals=3))
    return


if __name__ == '__main__':
    main()
