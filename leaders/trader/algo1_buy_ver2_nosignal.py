from algo_trader import *

class AlgoTrader1(AlgoTrader):
    def __init__(self):
        super().__init__(1)
        self.real_invest = True
        self.send_signal = False

        self.run()
        self.run_algo1()



if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = AlgoTrader1()
    window.show()
    app.exec_()