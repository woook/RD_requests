input_file=$1
job=$2
genome=$3
merge_file_name=$4
project_file=$(awk -F ' ' '{print $3":"$4}' $input_file)
to_download=($project_file)

# Download all vcfs
for i in "${to_download[@]}"
    do
        echo $i
        dx download $i
    done

# Index VCFs
echo "Indexing VCFs"
for vcf in $(ls *vcf.gz); do
    /home/dnanexus/bcftools-1.19/bcftools index $vcf;
done

# Normalising VCFs
mkdir norm
echo "Normalising VCFs"
for vcf in $(ls *vcf.gz); do
    /home/dnanexus/bcftools-1.19/bcftools norm -m -any -f ${genome} -Oz $vcf > norm/$vcf;
done

# Indexing normalised VCFs
echo "Indexing normalised VCFs"
cd norm
for vcf in $(ls *vcf.gz); do
    /home/dnanexus/bcftools-1.19/bcftools index -f $vcf;
done

# Merging normalised VCFs
echo "Merging normalised VCFs"
command="/home/dnanexus/bcftools-1.19/bcftools merge --output-type v -m none --missing-to-ref"
# Add the VCF files names to the command
for vcf in $(ls *vcf.gz); do
    command="${command} $vcf";
done
command="${command} > ../merged.vcf"
echo "${command}"
eval $command

# Bgzip and index merged VCF file
echo "Bgzip and indexing merged file"
cd ..
/home/dnanexus/htslib-1.19/bgzip merged.vcf
/home/dnanexus/bcftools-1.19/bcftools index merged.vcf.gz

export BCFTOOLS_PLUGINS=/home/dnanexus/bcftools-1.19/plugins/

# Final merging and processing
echo "Final processing"
command="/home/dnanexus/bcftools-1.19/bcftools norm -m -any -f ${genome} -Ou merged.vcf.gz"
command="${command} | /home/dnanexus/bcftools-1.19/bcftools +fill-tags --output-type v -o merge_tag.vcf -- -t AN,AC,NS,AF,MAF,AC_Hom,AC_Het,AC_Hemi"
command="${command} ; /home/dnanexus/bcftools-1.19/bcftools sort merge_tag.vcf -Oz > final_merged_${job}.vcf.gz"
command="${command} ; /home/dnanexus/htslib-1.19/tabix -p vcf final_merged_${job}.vcf.gz"
eval $command
dx upload final_merged_${job}.vcf.gz
dx upload final_merged_${job}.vcf.gz.tbi
dx terminate ${job}