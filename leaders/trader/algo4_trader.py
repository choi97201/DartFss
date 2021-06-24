from algo_trader import *

class AlgoTrader4(AlgoTrader):
    def __init__(self, real_invest):
        super().__init__(4)

        self.real_invest = real_invest

        self.run()
        self.run_algo4()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    real_invest = True
    window = AlgoTrader4(real_invest)
    window.show()
    app.exec_()