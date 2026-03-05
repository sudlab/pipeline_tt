from cgatcore import pipeline as P
from ruffus import originate, transform, merge, follows, mkdir
import os
import sys

PARAMS = P.get_parameters("pipeline.yml")

@merge((PARAMS["target_genome"], PARAMS["spikein_genome"]),
        "merged_genome.fa")
def merge_genomes(infiles, outfile):
    ''' This function merges together the target genome and the spike=in genome ready
    for indexing'''

    target, spikein = infiles

    statement = '''
    sed 's/^>/>spikein_/' %(spikein)s 
    | cat - %(target)s > %(outfile)s'''
    
    P.run(statement)

@merge((PARAMS["target_geneset"], PARAMS["spikein_geneset"]),
        "merged_geneset.gtf.gz")
def merge_genesets(infiles, outfile):
    ''' This function merges together the target genome and the spike=in genome ready
    for indexing'''

    target, spikein = infiles

    statement = '''
    sed 's/^/spikein_/' %(spikein)s
    | cat - %(target)s
    | gzip > %(outfile)s'''
    
    P.run(statement)

@follows(mkdir("star_index.dir"))
@merge((merge_genomes, merge_genesets),
       "star_index.dir/SA")
def index_genome(infiles, outfile):
    ''' THis function indexes the new merged genome using STAR and
    outputs the index into the star index directory'''

    fasta, geneset = infiles
    logfile = "star_index.log"
    outdir = os.path.dirname(outfile)

    statement = '''
    zcat %(geneset)s > %(outdir)s/geneset.gtf &&

    STAR --runMode genomeGenerate 
         --genomeDir %(outdir)s 
         --genomeFastaFiles %(fasta)s 
         --sjdbGTFfile %(outdir)s/geneset.gtf
         --sjdbOverhang %(star_overhang)s
         --runThreadN %(star_threads)s
         --genomeSAsparseD 2
         --genomeSAindexNbases 10 > %(logfile)s &&
         
    rm %(outdir)s/geneset.gtf'''

    P.run(statement, job_threads=PARAMS["star_threads"], job_memory=PARAMS["star_index_memory"])

@follows(mkdir("star.dir"))
@collate("*.fastq.?.gz",
         regex("(.+).fastq...gz"),
         add_inputs(index_genome),
         r"star.dir/\1.bam")
def map_with_star(infiles, outfile):
    read1, read2, index = infiles

    ...stuff here...



    
P.main(sys.argv)
