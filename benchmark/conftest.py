def pytest_addoption(parser):
    parser.addoption("--dbname", action="store", default="rocks_db")
    parser.addoption("--num", action="store", default=1000)
    parser.addoption("--k_size", action="store", default=16)
    parser.addoption("--v_size", action="store", default=100)
    parser.addoption("--batch_size", action="store", default=100)
    parser.addoption("--percent", action="store", default=0.01)
