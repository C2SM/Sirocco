"""Patch FirecrestTransport to handle dangling symlinks.

Sirocco uses pre-submission with SLURM dependencies, which means CalcJobs are
submitted before their parent jobs complete. This can result in symlinks being
created to targets that don't exist yet.

SSH transport allows creating symlinks to non-existent targets (standard POSIX
behavior), but FirecREST validates that the target exists before creating the
symlink. This patch adds a fallback mechanism that creates symlinks via tar
archive extraction when the target doesn't exist, matching SSH behavior.

This is a Sirocco-specific workaround and not intended for upstream aiida-firecrest.
"""

import logging
import os
import stat
import tarfile
import tempfile
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)


def patch_firecrest_symlink():
    """Apply monkey-patch to FirecrestTransport.symlink_async.

    This patch adds a fallback mechanism for creating symlinks when the target
    doesn't exist yet, which is needed for Sirocco's pre-submission workflow.
    """
    try:
        from aiida_firecrest.transport import FirecrestTransport
        from aiida_firecrest.utils import FcPath, convert_header_exceptions
        from firecrest.FirecrestException import UnexpectedStatusException
    except ImportError:
        # aiida-firecrest not installed, skip patching
        logger.debug("aiida-firecrest not installed, skipping symlink patch")
        return

    # Store reference to original method
    original_symlink_async = FirecrestTransport.symlink_async

    async def _create_symlink_archive(
        self, source_path: str, link_path: FcPath
    ) -> None:
        """Create a symlink without validating the target by extracting an archive.

        Args:
            source_path: Absolute path to symlink target
            link_path: Absolute path where symlink should be created
        """
        _ = uuid.uuid4()
        remote_archive = self._temp_directory.joinpath(f"symlink_{_}.tar.gz")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            local_symlink = tmp_dir_path.joinpath(link_path.name)
            os.symlink(source_path, local_symlink)

            archive_path = tmp_dir_path.joinpath("symlink.tar.gz")
            with tarfile.open(archive_path, "w:gz", dereference=False) as tar:
                tar.add(local_symlink, arcname=link_path.name)

            await self.putfile_async(archive_path, remote_archive)

        try:
            with convert_header_exceptions():
                await self.async_client.extract(
                    self._machine, str(remote_archive), str(link_path.parent)
                )
        finally:
            await self.remove_async(remote_archive)

    async def patched_symlink_async(self, remotesource, remotedestination):
        """Create symlink with fallback for missing targets.

        This patched version attempts the standard symlink operation first, and
        if it fails due to a missing target, falls back to creating the symlink
        via tar archive extraction.

        Args:
            remotesource: Absolute path to symlink target
            remotedestination: Absolute path where symlink should be created
        """
        from pathlib import PurePosixPath

        link_path = FcPath(remotedestination)
        source_path = str(remotesource)

        if not PurePosixPath(source_path).is_absolute():
            raise ValueError("target(remotesource) must be an absolute path")
        if not PurePosixPath(str(link_path)).is_absolute():
            raise ValueError("link(remotedestination) must be an absolute path")

        try:
            # Try standard symlink first
            with convert_header_exceptions():
                await self.async_client.symlink(
                    self._machine, source_path, str(link_path)
                )
            return
        except (FileNotFoundError, UnexpectedStatusException) as exc:
            # FirecREST checks that the symlink target exists; fall back to a creation
            # path that tolerates missing targets (matching SSH behaviour).
            # Handle both FileNotFoundError and 404 responses from symlink validation
            if isinstance(exc, UnexpectedStatusException):
                # Only use fallback for 404 errors (target doesn't exist)
                if "404" not in str(exc):
                    raise
            if not await self.path_exists_async(link_path.parent):
                raise

            # Check if symlink already exists
            try:
                existing_stat = await self._lstat(link_path)
                if stat.S_ISLNK(existing_stat.st_mode):
                    # It's a symlink - assume it's correct from a retry, skip creation
                    logger.debug(
                        f"Symlink {link_path} already exists, skipping creation"
                    )
                    return
                else:
                    # Not a symlink, it's a file or directory - error
                    raise FileExistsError(
                        f"'{link_path}' already exists and is not a symlink"
                    )
            except FileNotFoundError:
                # Doesn't exist yet, proceed with creation
                pass

        # Fallback: create symlink via tar archive
        logger.debug(
            f"Creating symlink {link_path} -> {source_path} via tar archive "
            f"(target doesn't exist yet)"
        )
        await _create_symlink_archive(self, source_path, link_path)

    # Apply the patch
    FirecrestTransport.symlink_async = patched_symlink_async
    logger.info(
        "Applied FirecREST symlink patch for Sirocco pre-submission workflows"
    )
