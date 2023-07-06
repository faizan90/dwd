'''
@author: Faizan-Uni-Stuttgart

24 Feb 2020

14:48:59

'''
import os
import time
import timeit
import subprocess
from pathlib import Path
from shutil import copy2

import zipfile

DEBUG_FLAG = True


def main():

    main_dir = Path(r'P:\dwd_meteo')
    os.chdir(main_dir)

    in_dir = Path(r'zipped_DWD_data')
    out_dir = Path(r'hourly/txt__raw_dwd_data')

    # Using zipfile instead.
    zip_exe = None  # Path(r'C:\Program Files\7-Zip\7z.exe')

    in_ext = '.zip'

    wild_card = '.txt'
    #==========================================================================

    os.chdir(main_dir)

    if zip_exe is not None:
        assert zip_exe.exists(), '7-Zip executable not found!'

        zip_exe = str(zip_exe)

    out_dir.mkdir(exist_ok=True, parents=True)

    for root, *_, files in os.walk(in_dir):
        if not files:
            continue

        curr_out_dir = out_dir / os.path.basename(root)

        curr_out_dir.mkdir(exist_ok=True)

        for file in files:
            curr_in_path = str(Path(root) / file)

            if not (file[-len(in_ext):] == in_ext):
                curr_out_path = curr_out_dir / file
                copy2(curr_in_path, curr_out_path)

            else:
                curr_out_path = str(curr_out_dir / (file.split('.')[0]))

                if False:
                    arg = (
                        zip_exe +
                        " -y e " +
                        curr_in_path +
                        " -o" +
                        curr_out_path +
                        " *" +
                        wild_card +
                        " -aoa -r -mmem=1000m")

                    subprocess.call(arg)

                else:
                    # with py7zr.SevenZipFile(curr_in_path, 'r') as archive:
                    #     archive.extractall(path=curr_out_path)

                    with zipfile.ZipFile(curr_in_path, 'r') as zip_ref:
                        zip_ref.extractall(curr_out_path)

    return


if __name__ == '__main__':

    print('#### Started on %s ####\n' % time.asctime())
    START = timeit.default_timer()

    #==========================================================================
    # When in post_mortem:
    # 1. "where" to show the stack
    # 2. "up" move the stack up to an older frame
    # 3. "down" move the stack down to a newer frame
    # 4. "interact" start an interactive interpreter
    #==========================================================================

    if DEBUG_FLAG:
        try:
            main()

        except:
            import pdb
            pdb.post_mortem()

    else:
        main()

    STOP = timeit.default_timer()
    print(('\n#### Done with everything on %s.\nTotal run time was'
           ' about %0.4f seconds ####' % (time.asctime(), STOP - START)))
