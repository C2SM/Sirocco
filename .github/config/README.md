The `slurm_rsa` is a private key that provides access to the `slurm-ssh` container via the `xenonmiddleware/slurm:17`
image, which is already part of the authorized keys in the container. Don't re-create that or create it yourself!
Otherwise you will be subject to enduring pain.
