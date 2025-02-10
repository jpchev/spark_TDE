package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class SeekableCipherInputStream extends InputStream implements Seekable, PositionedReadable {
    private static final int AES_BLOCK_SIZE = 16; // AES block size in bytes (128 bits)
    public static final String TRANSFORMATION = "AES/CTR/NoPadding";

    private final FSDataInputStream baseStream;
    private final Cipher cipher;
    private final byte[] initialIv;
    private final SecretKey secretKey;
    private long position = 0;
    private int readBlockNumber = -1;
    private final byte[] encryptedBlock = new byte[AES_BLOCK_SIZE];
    private byte[] decryptedBlock = null;

    public SeekableCipherInputStream(FSDataInputStream baseStream, SecretKey secretKey, byte[] iv) throws IOException {
        this.baseStream = baseStream;
        this.secretKey = secretKey;
        this.initialIv = iv;

        try {
            this.cipher = Cipher.getInstance(TRANSFORMATION);
        } catch (Exception e) {
            throw new IOException("Failed to initialize cipher", e);
        }
    }

    @Override
    public void seek(long targetPos) throws IOException {
        if (targetPos < 0) {
            throw new IllegalArgumentException("Position cannot be negative.");
        }
        this.position = targetPos;
        // set to the beginning of the block
        baseStream.seek(targetPos / AES_BLOCK_SIZE * AES_BLOCK_SIZE);
        updateIvForPosition(targetPos);
    }

    private void updateIvForPosition(long pos) throws IOException {
        // Compute new IV based on block number
        long blockNumber = pos / AES_BLOCK_SIZE;

        byte[] newIv = Arrays.copyOf(initialIv, initialIv.length);
        addLongToByteArray(newIv, blockNumber);

        IvParameterSpec newIvSpec = new IvParameterSpec(newIv);
        setIVInCipher(newIvSpec);
    }

    private static void addLongToByteArray(byte[] byteArray, long valueToAdd) {
        for (int i = 0; i < valueToAdd; i++) {
            increment(byteArray);
        }
    }

    private static void increment(byte[] b) {
        int n = b.length - 1;
        while ((n >= 0) && (++b[n] == 0)) {
            n--;
        }
    }

    private void setIVInCipher(IvParameterSpec ivSpec) throws IOException {
        try {
            cipher.init(Cipher.DECRYPT_MODE, secretKey, ivSpec);
        } catch (Exception e) {
            throw new IOException("Failed to reset cipher for new IV", e);
        }
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public int read(long pos, byte[] buffer, int offset, int length) throws IOException {
        seek(pos);
        return read(buffer, offset, length);
    }

    @Override
    public void readFully(long pos, byte[] buffer, int offset, int length) throws IOException {
        seek(pos);
        int bytesRead = read(buffer, offset, length);
        if (bytesRead < length) {
            throw new EOFException("Not enough data available for readFully.");
        }
    }

    @Override
    public void readFully(long pos, byte[] buffer) throws IOException {
        readFully(pos, buffer, 0, buffer.length);
    }

    @Override
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        int bytesRead = read(buffer, 0, 1);
        return (bytesRead == -1) ? -1 : buffer[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        // load and decrypt a new block if needed
        if (readBlockNumber != position / AES_BLOCK_SIZE) {
            try {
                if (readDecryptAESBlock() == -1) {
                    return -1;
                }
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new RuntimeException(e);
            }
        }

        int skip = (int) position % AES_BLOCK_SIZE;
        if (decryptedBlock.length == skip) {
            return -1;
        }

        int leftInBlock = decryptedBlock.length - (int) position % AES_BLOCK_SIZE;
        int copySize = min(len, b.length, leftInBlock);

        System.arraycopy(decryptedBlock, skip, b, off, copySize);
        position += copySize;
        return copySize;
    }

    private int readDecryptAESBlock() throws IOException, IllegalBlockSizeException, BadPaddingException {
        // load block in memory
        int bytesRead = baseStream.read(encryptedBlock, 0, AES_BLOCK_SIZE);
        if (bytesRead == -1) {
            return -1; // End Of File
        }
        int totalBLockBytesRead = bytesRead;
        while (totalBLockBytesRead < encryptedBlock.length && bytesRead > 0) {
            bytesRead = baseStream.read(encryptedBlock, totalBLockBytesRead, AES_BLOCK_SIZE - totalBLockBytesRead);
            if (bytesRead == -1) {
                break;  // End Of File
            }
            totalBLockBytesRead += bytesRead;
        }
        readBlockNumber = (int) (position / AES_BLOCK_SIZE);
        updateIvForPosition(position);

        // decrypt block
        if (totalBLockBytesRead > 0) {
            decryptedBlock = cipher.doFinal(encryptedBlock, 0, totalBLockBytesRead);
        }
        return totalBLockBytesRead;
    }

    private static int min(int... numbers) {
        if (numbers.length == 0) {
            throw new IllegalArgumentException("At least one number must be provided");
        }

        int min = numbers[0];
        for (int num : numbers) {
            if (num < min) {
                min = num;
            }
        }
        return min;
    }

    @Override
    public void close() throws IOException {
        baseStream.close();
    }
}

