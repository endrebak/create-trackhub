from os.path import splitext, basename

import pandas as pd

from snakemake import shell

shell.executable("bash")

configfile: "config.yaml"

project_folder = config["project_folder"]
genomes = config["genomes"]

ss = pd.read_table(config["sample_sheet"], sep="\s+", header=0)

rule targets:
    input:
        expand("{project_folder}/genomes.txt", project_folder=project_folder),
        expand("{project_folder}/hub.txt", project_folder=project_folder),
        expand("{project_folder}/{genome}/trackDb.txt", project_folder=project_folder, genome=genomes)


rule genome_file:
    output:
        "{path}/genomes.txt"
    run:
        contents = ["genome {genome}".format(genome=config["genomes"][0])]
        for genome in config["genomes"]:
            line = "trackDb {genome}/trackDb.txt".format(genome=genome)
            contents.append(line)

        with open(output[0], "w+") as f:
            f.write("\n".join(contents))


rule hub_file:
    input:
        genomes_txt = "{project_folder}/genomes.txt"
    output:
        "{project_folder}/hub.txt"
    run:
        contents = """hub {hub_name}
shortLabel {short_hub_label}
longLabel {long_hub_label}
genomesFile genomes.txt
email {email}
descriptionUrl {hub_description_url}"""

        contents = contents.format(**config)
        contents = contents.format(genomes_txt=input.genomes_txt)

        with open(output[0], "w+") as f:
            f.write(contents)


rule trackdb_file:
    output:
        "{project_folder}/{genome}/trackDb.txt"
    run:
        header_contents = """track {supertrack}
superTrack on
group regulation
shortLabel {short_label_supertrack}
longLabel {long_label_supertrack}"""

        group_contents = """track {group}
parent {supertrack} on
visibility dense
container multiWig
aggregate none
showSubtrackColorOnUi on
type bigWig 0 1000
autoScale on
viewLimits 0:20
maxHeighPixels 100:16:8
shortLabel {group_short_label}
longLabel {group_long_label}"""

        track_contents = """track {track_name}
shortLabel {track_label}
parent {group} on
type {track_type}
color {color}
bigDataUrl {track_file}"""

        groups = ss.Group.drop_duplicates()
        supertrack = config["supertrack"]

        ext_to_type = {".bw": "bigWig 0 1000",
                       ".bigwig": "bigWig 0 1000",
                       ".bed": "bed 3"}

        contents = [header_contents.format(**config)]

        for group, df in ss.groupby("Group"):

            group_short_label = config["group"][group]["short_label"]
            group_long_label = config["group"][group]["long_label"]

            contents.append(group_contents.format(**vars()))

            for _, row in df.iterrows():
                d = row.to_dict()
                track_file = d["File"]
                extension = splitext(basename(track_file))[1]
                track_name = d["Name"]
                color = d["Color"]

                track_label = track_name.replace("_", " ")
                track_type = ext_to_type[extension]

                contents.append(track_contents.format(**vars()))

        with open(output[0], "w+") as f:
            f.write("\n\n".join(contents))
