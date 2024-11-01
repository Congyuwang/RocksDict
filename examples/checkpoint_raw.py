from rocksdict import Rdict, Options, Checkpoint

def run_checkpoint_raw_example():
    opt = Options(True)  # Enable raw mode
    opt.create_if_missing(True)
    path = "./temp_checkpoint_raw_db_example"
    checkpoint_path = "./temp_checkpoint_raw_example"

    # Initialize the database
    test_dict = Rdict(path, opt)

    # Populate the database
    for i in range(1000):
        test_dict.put_entity(bytes(i), names=[b"value"], values=[bytes(i * i)])

    # Create a checkpoint
    checkpoint = Checkpoint(test_dict)
    checkpoint.create_checkpoint(checkpoint_path)
    del checkpoint

    # Open the checkpoint as a new Rdict instance
    checkpoint_dict = Rdict(checkpoint_path)

    # Verify the checkpoint data
    for i in range(1000):
        assert bytes(i) in checkpoint_dict
        entity = checkpoint_dict.get_entity(bytes(i))
        assert entity == [(b"value", bytes(i * i))]

    checkpoint_dict.close()
    test_dict.close()

    # Cleanup
    Rdict.destroy(path, opt)
    Rdict.destroy(checkpoint_path, opt)

if __name__ == "__main__":
    run_checkpoint_raw_example()
