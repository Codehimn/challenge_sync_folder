import json
import os
import shutil
import tempfile

from veeam_task import (calculate_md5, create_or_update_file, read_metadata, remove_file_or_folder,
                        sync_folders, write_metadata)


def test_calculate_md5():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"Hello, world!")
        temp_file.seek(0)
        assert calculate_md5(temp_file.name) == '6cd3556deb0da54bca060b4c39479839'


def test_read_metadata():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(b"1234")
        temp_file.seek(0)
        assert read_metadata(temp_file.name) == 1234


def test_write_metadata():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        write_metadata(temp_file.name, 1234)
        temp_file.seek(0)
        data = json.loads(temp_file.read().decode('utf-8'))
        assert data['bytes_transferred'] == 1234


def test_create_or_update_file():
    with tempfile.NamedTemporaryFile(delete=False) as src_file, tempfile.NamedTemporaryFile(delete=False) as dest_file:
        src_file.write(b"Hello, world!")
        src_file.seek(0)
        create_or_update_file(src_file.name, dest_file.name)
        dest_file.seek(0)
        assert dest_file.read() == b"Hello, world!"


def test_remove_file_or_folder():
    temp_dir = tempfile.mkdtemp()
    assert os.path.exists(temp_dir)
    remove_file_or_folder(temp_dir)
    assert not os.path.exists(temp_dir)


def test_sync_folders():
    src_temp_dir = tempfile.mkdtemp()
    replica_temp_dir = tempfile.mkdtemp()

    try:
        with open(os.path.join(src_temp_dir, 'file1.txt'), 'w') as f:
            f.write('Hello, world!')

        os.makedirs(os.path.join(src_temp_dir, 'subfolder'))
        with open(os.path.join(src_temp_dir, 'subfolder', 'file2.txt'), 'w') as f:
            f.write('Another file')

        sync_folders(src_temp_dir, replica_temp_dir)

        assert os.path.exists(os.path.join(replica_temp_dir, 'file1.txt'))
        assert os.path.exists(os.path.join(replica_temp_dir, 'subfolder', 'file2.txt'))

        with open(os.path.join(replica_temp_dir, 'file1.txt'), 'r') as f:
            assert f.read() == 'Hello, world!'

        with open(os.path.join(replica_temp_dir, 'subfolder', 'file2.txt'), 'r') as f:
            assert f.read() == 'Another file'

    finally:
        shutil.rmtree(src_temp_dir)
        shutil.rmtree(replica_temp_dir)
