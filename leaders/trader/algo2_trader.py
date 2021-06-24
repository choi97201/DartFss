from algo_trader import *

class AlgoTrader2(AlgoTrader):
    def __init__(self, real_invest):
        super().__init__(2)

        self.real_invest = real_invest
        self.send_signal = True

        self.run()
        self.run_algo2()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    real_invest = True
    window = AlgoTrader2(real_invest)
    window.show()
    app.exec_()