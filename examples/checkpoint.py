from rocksdict import Rdict, Options, Checkpoint

def run_checkpoint_example():
    opt = Options()
    opt.create_if_missing(True)
    path = "./temp_checkpoint_db_example"
    checkpoint_path = "./temp_checkpoint_example"

    # Initialize the database
    test_dict = Rdict(path, opt)

    # Populate the database
    for i in range(1000):
        test_dict[i] = i * i

    # Create a checkpoint
    checkpoint = Checkpoint(test_dict)
    checkpoint.create_checkpoint(checkpoint_path)
    del checkpoint

    # Open the checkpoint as a new Rdict instance
    checkpoint_dict = Rdict(checkpoint_path)

    # Verify the checkpoint data
    for i in range(1000):
        assert i in checkpoint_dict
        assert checkpoint_dict[i] == i * i

    checkpoint_dict.close()
    del test_dict

    # Cleanup
    Rdict.destroy(path, opt)
    Rdict.destroy(checkpoint_path, opt)

if __name__ == "__main__":
    run_checkpoint_example()
