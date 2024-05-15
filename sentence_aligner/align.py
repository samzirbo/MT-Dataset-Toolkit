import json
from tqdm import tqdm
import subprocess
import os
from bertalign.aligner import Bertalign

def align_corpus(
    input_file: str,
    output_file: str,
    gender: bool = True,
    src_lang: str = "en",
    tgt_lang: str = "es",
    no_talks: int = None,
    offset: int = 0,
    max_align: int = 5,
    top_k: int = 3,
    win: int = 5,
    skip: float = -0.1,
    margin: bool = True,
    len_penalty: bool = True,
    is_split: bool = False
):
    if not output_file:
        input_fn = input_file.split(".")
        input_fn.insert(-1, "aligned")
        output_file = ".".join(input_fn)

    # check if the output file exists
    if not os.path.exists(output_file):
        os.system(f"touch {output_file}")

    total_talks = int(subprocess.run(["wc", "-l", output_file], capture_output=True, text=True, check=True).stdout.split()[0])
    no_talks = total_talks if no_talks is None else no_talks

    # read the output file to get the ids of talks already aligned
    with open(output_file, "r") as f:
        aligned_talks = set([json.loads(line)['TALK-ID'] for line in f])

    initial_done = len(aligned_talks)

    # read the input file
    with open(input_file, "r") as fin:
        for idx, line in tqdm(enumerate(fin), total=offset + no_talks):
            if idx < offset:
                continue

            talk = json.loads(line)
            talk_id = talk['TALK-ID']
            talk_name = talk['TALK-NAME']
            talk_gender = talk['GENDER'] if gender else None

            if talk_id in aligned_talks:
                continue

            src = talk['TRANSCRIPTS'][src_lang]
            tgt = talk['TRANSCRIPTS'][tgt_lang]

            # align the sentences
            aligner = Bertalign(
                src, tgt,
                src_lang=src_lang,
                tgt_lang=tgt_lang,
                max_align=max_align,
                top_k=top_k,
                win=win,
                skip=skip,
                margin=margin,
                len_penalty=len_penalty,
                is_split=is_split
            )

            aligner.align_sents()

            # write the aligned sentences to the output file
            with open(output_file, "a") as fout:
                for src, tgt in aligner.get_sentences():
                    item = {"TALK-ID": talk_id, "TALK-NAME": talk_name}
                    if gender:
                        item['GENDER'] = talk_gender
                    item[src_lang.upper()] = src
                    item[tgt_lang.upper()] = tgt
                    fout.write(json.dumps(item) + "\n")

            aligned_talks.add(talk_id)

            if len(aligned_talks) >= initial_done + no_talks:
                break

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Align the sentences in the input file using the bertalign model")
    parser.add_argument("--INPUT", type=str, help="The input file containing the sentences to align")
    parser.add_argument("--OUTPUT", type=str, help="The output file to write the aligned sentences")
    parser.add_argument("--GENDER", type=bool, default=False, help="The data containes gender information")
    parser.add_argument("--SRC_LANG", type=str, default="en", help="The source language")
    parser.add_argument("--TGT_LANG", type=str, default="es", help="The target language")
    parser.add_argument("--NO_TALKS", type=int, default=None, help="The number of talks to align")
    parser.add_argument("--OFFSET", type=int, default=0, help="The offset to start aligning the talks")
    parser.add_argument("--MAX_ALIGN", type=int, default=5, help="The maximum number of alignments")
    parser.add_argument("--TOP_K", type=int, default=3, help="The top k alignments")
    parser.add_argument("--WIN", type=int, default=5, help="The window size")
    parser.add_argument("--SKIP", type=float, default=-0.1, help="The skip value")
    parser.add_argument("--MARGIN", type=bool, default=True, help="The margin value")
    parser.add_argument("--LEN_PENALTY", type=bool, default=True, help="The length penalty value")
    parser.add_argument("--IS_SPLIT", type=bool, default=False, help="The split value")

    args = parser.parse_args()

    align_corpus(
        args.INPUT,
        args.OUTPUT,
        args.GENDER,
        src_lang=args.SRC_LANG,
        tgt_lang=args.TGT_LANG,
        no_talks=args.NO_TALKS,
        offset=args.OFFSET,
        max_align=args.MAX_ALIGN,
        top_k=args.TOP_K,
        win=args.WIN,
        skip=args.SKIP,
        margin=args.MARGIN,
        len_penalty=args.LEN_PENALTY,
        is_split=args.IS_SPLIT
    )