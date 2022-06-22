
import pandas as pd
import numpy as np
from ib_insync import *

import warnings
warnings.filterwarnings("ignore")

# %%
ib = IB()

# %%
ib.connect('127.0.0.1', 7497, clientId= 21)

# %%
market = 'YM'
# Create Contract
contract = ContFuture('YM', 'ECBOT')
ib.qualifyContracts(contract)

account = "DU5317735"

# %%
data = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='9 D', # la duracion del historico requerido dependera del mayor de los parametros de los indicadores usados en la estrategia
    barSizeSetting='1 hour',
    whatToShow='MIDPOINT',
    useRTH=False,
    keepUpToDate=True,
    formatDate=1)  

# %%
def on_new_bar(bars: BarDataList, has_new_bar: bool): 
    if has_new_bar: # cada vez que hay nuva barra:
        
        # 1. obtener los datos ohlcv
        df = util.df(data)
        df = df.drop(['volume', 'average', 'barCount'], axis=1) # eliminamos las columnas volume, average y barCount 

        # 2. añadimos los indicadores de la estrategia
        df['atr_28'] = np.max(pd.concat([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift()), np.abs(df['low'] - df['close'].shift())], axis=1), axis=1).rolling(28).sum()/28
        df['atr_200'] = np.max(pd.concat([df['high'] - df['low'], np.abs(df['high'] - df['close'].shift()), np.abs(df['low'] - df['close'].shift())], axis=1), axis=1).rolling(200).sum()/200
        df['three_bars_down'] = (df['close'] < df['open']) & (df['close'].shift() < df['open'].shift()) & (df['close'].shift(2) < df['open'].shift(2))
        df['bb_upband_20__2'] = df['close'].rolling(20).mean() + (df['close'].rolling(20).std() * 2)

        # añadimos las condiciones
        df['in_condition_1'] = df['close'] > df['close'].shift(2) + + 1.3 * df['atr_28']
        df['in_condition_2'] = df['close'] > df['close'].shift(2) + 1.3 * df['atr_200']
        df['out_condition_1'] = df['three_bars_down']
        df['out_condition_2'] = df['close'] < df['bb_upband_20__2']

        df["signal_in"] = np.where(df['in_condition_1'] & df['in_condition_2'], 1, 0)
        df["signal_out"] = np.where(df['out_condition_1'] | df['out_condition_2'], -1, 0)


        # guardamos los datos en el df en un pickle
        df.to_pickle('df_data_21.pkl') 

        # hacemos una copia de df a current_bar 
        current_bar = df.copy()
        # y nos quedamos solo con la ultima fila
        current_bar = current_bar.tail(1)

        # leemos el df_out
        df_out = pd.read_pickle('df_out_21.pkl')

        if len(df_out) == 0:
            current_bar['order_sent'] = 'none'
            current_bar['filled_price'] = 0.0
            current_bar['open_pos'] = False

        if len(df_out) > 0:

            # obtenemos la posicion previa a la orden y pnl
            portfolio = ib.portfolio()
            portfolio_df = pd.DataFrame(portfolio)

            if len(portfolio_df) > 0:
                portfolio_df['symbol'] = portfolio_df.contract.apply(lambda x: x.symbol)
                portfolio_df['position'] = portfolio_df.position
                portfolio_df['realizedPNL'] = portfolio_df.realizedPNL

                for i in range(len(portfolio_df)):
                    if market in portfolio_df.symbol[i]:
                        current_bar['pos_before_order'] = portfolio_df.position[i]
                        current_bar['pnl'] = portfolio_df.realizedPNL[i]
                        break
                    else:
                        current_bar['pos_before_order'] = 0
                        current_bar['pnl'] = 0
            else:
                current_bar['pos_before_order'] = 0
                current_bar['pnl'] = 0

            # ponemos todas las combinaciones posibles para que no de error
            if ((df['signal_in'].iloc[-2] == 0 ) & (df['signal_out'].iloc[-2] == 0) & (df_out['open_pos'].iloc[-1] == False)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1]

            if ((df['signal_in'].iloc[-2] == 0 ) & (df['signal_out'].iloc[-2] == 0) & (df_out['open_pos'].iloc[-1] == True)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1]    

            if ((df['signal_in'].iloc[-2] == 1 ) & (df['signal_out'].iloc[-2] == 0) & (df_out['open_pos'].iloc[-1] == True)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1] 

            if ((df['signal_in'].iloc[-2] == 1 ) & (df['signal_out'].iloc[-2] == -1) & (df_out['open_pos'].iloc[-1] == True)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1]    

            if ((df['signal_in'].iloc[-2] == 1 ) & (df['signal_out'].iloc[-2] == -1) & (df_out['open_pos'].iloc[-1] == False)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1]

            if ((df['signal_in'].iloc[-2] == 0 ) & (df['signal_out'].iloc[-2] == -1) & (df_out['open_pos'].iloc[-1] == False)):
                current_bar['order_sent'] = 'none'
                current_bar['filled_price'] = 0.0
                current_bar['open_pos'] = df_out['open_pos'].iloc[-1]


            # lanzaremos orden a mercado de venta si en la penultima barra, signal_out es -1 y habia posicion abierta
            if ((df['signal_in'].iloc[-2] == 0 ) & (df['signal_out'].iloc[-2] == -1) & (df_out['open_pos'].iloc[-1] == True)):
                order = MarketOrder('SELL', 2)
                trade = ib.placeOrder(contract, order)
                current_bar['order_sent'] = 'SELL'

                current_bar['open_pos'] = False
                #df_fills = pd.DataFrame(ib.fills())
                #current_bar['filled_price'] = df_fills.iloc[-1].tolist()[1].price

            # lanzaremos orden a mercado de compra si en la penultima barra, signal_in es 1 y no habia posicion abierta
            if ((df['signal_in'].iloc[-2] == 1 ) & (df['signal_out'].iloc[-2] == 0) & (df_out['open_pos'].iloc[-1] == False)):
                order = MarketOrder('BUY', 2)
                trade = ib.placeOrder(contract, order)
                current_bar['order_sent'] = 'BUY'

                current_bar['open_pos']= True
                #df_fills = pd.DataFrame(ib.fills())
                #current_bar['filled_price'] = df_fills.iloc[-1].tolist()[1].price


        #  actualizamos con la ultima linea
        df_out = df_out.append(current_bar, ignore_index=True)

        # y actualizamos los precios que antes erean 0
        if len(df_out) > 1:
            df_out['close'].iloc[-2] = df['close'].iloc[-2]
            df_out['open'].iloc[-2] = df['open'].iloc[-2]
            df_out['high'].iloc[-2] = df['high'].iloc[-2]
            df_out['low'].iloc[-2] = df['low'].iloc[-2]

            df_out['atr_28'].iloc[-2] = df['atr_28'].iloc[-2]
            df_out['atr_200'].iloc[-2] = df['atr_200'].iloc[-2]
            df_out['three_bars_down'].iloc[-2] = df['three_bars_down'].iloc[-2]
            df_out['bb_upband_20__2'].iloc[-2] = df['bb_upband_20__2'].iloc[-2]

            df_out['in_condition_1'].iloc[-2] = df['in_condition_1'].iloc[-2]
            df_out['in_condition_2'].iloc[-2] = df['in_condition_2'].iloc[-2]
            df_out['out_condition_1'].iloc[-2] = df['out_condition_1'].iloc[-2]
            df_out['out_condition_2'].iloc[-2] = df['out_condition_2'].iloc[-2]
            df_out["signal_in"].iloc[-2] = df['signal_in'].iloc[-2]
            df_out["signal_out"].iloc[-2] = df['signal_out'].iloc[-2]

        df_out.to_pickle('df_out_21.pkl')


# Set callback function for streaming bars
data.updateEvent += on_new_bar

#ib.sleep(600)
#ib.cancelHistoricalData(data)

# Run infinitely 
ib.run()

