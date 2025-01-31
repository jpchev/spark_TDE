package my.custom.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.util.Progressable;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Base64;

/**
 * This class extends the S3AFileSystem class and overrides the create and open methods.
 * It encrypts parquet files when writing and decrypts parquet files when reading.
 * Files not ending with the .parquet extension are not encrypted, nor decrypted.
 * The encryption algorithm used is AES-256 in CTR mode, because it is suitable for the "seekable" feature.
 * The key and initialization vector (IV), used to initialize the Cipher object, are passed through the Spark configuration object.
 *
 * The key size must be 256 bits (32 bytes),
 * AES blocks are 16 bytes large (128 bits), the last block can be under 16 bytes.
 * The initialization vector (IV) must be 16 bytes long (128 bits).
 *
 * The IV is being increment by 1 when switching to the next consecutive block.
 */
public class EncryptedS3AFileSystem extends S3AFileSystem {

    private static final String CIPHER_TRANSFORM = "AES/CTR/NoPadding";

    private static final String PARQUET_FILE_EXTENSION = ".parquet";

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        super.initialize(name, conf);
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission,
                                     boolean overwrite, int bufferSize,
                                     short replication, long blockSize,
                                     Progressable progress) throws IOException {
        if (path.getName().endsWith(PARQUET_FILE_EXTENSION)) {
            return encryptStream(super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress));
        } else {
            return super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
        }
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path path, FsPermission permission,
                                                 boolean overwrite, int bufferSize,
                                                 short replication, long blockSize,
                                                 Progressable progress) throws IOException {
        if (path.getName().endsWith(PARQUET_FILE_EXTENSION)) {
            return encryptStream(super.createNonRecursive(path, permission, overwrite, bufferSize, replication, blockSize, progress));
        } else {
            return super.createNonRecursive(path, permission, overwrite, bufferSize, replication, blockSize, progress);
        }
    }

    public FSDataOutputStream encryptStream(FSDataOutputStream baseStream) throws IOException {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_TRANSFORM);
            Configuration conf = getConf();
            String encryptionKeyBase64 = conf.get("aes.key");
            String ivBase64 = conf.get("aes.iv");
            SecretKey secretKey = base64toKey(encryptionKeyBase64);
            byte[] iv = base64toBytes(ivBase64);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new IvParameterSpec(iv));
            return new FSDataOutputStream(new CipherOutputStream(baseStream, cipher), null);
        } catch (Exception e) {
            throw new IOException("Error initializing encryption", e);
        }
    }

    private SecretKey generateEncryptionKey() throws IOException {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return keyGen.generateKey();
        } catch (Exception e) {
            throw new IOException("Failed to generate encryption key", e);
        }
    }

    // Convert SecretKey to Base64 string for storage
    private String keyToBase64(SecretKey secretKey) {
        return bytesToBase64(secretKey.getEncoded());
    }

    private SecretKey base64toKey(String secretKeyBase64) {
        return new SecretKeySpec(Base64.getDecoder().decode(secretKeyBase64), "AES");
    }

    private String bytesToBase64(byte[] b) {
        return Base64.getEncoder().encodeToString(b);
    }

    private byte[] base64toBytes(String base64str) {
        return Base64.getDecoder().decode(base64str);
    }

    @Override
    public FSDataInputStream open(Path path) throws IOException {
        if (path.getName().endsWith(PARQUET_FILE_EXTENSION)) {
            FSDataInputStream encryptedStream = super.open(path);
            InputStream decryptedStream = decryptStream(encryptedStream);
            return new FSDataInputStream(decryptedStream);
        } else {
            return super.open(path);
        }
    }

    // Decrypt the stream
    private InputStream decryptStream(FSDataInputStream encryptedStream) throws IOException {
        Configuration conf = getConf();
        String encryptionKeyBase64 = conf.get("aes.key");
        String ivBase64 = conf.get("aes.iv");

        SecretKey encryptionKey = base64toKey(encryptionKeyBase64);
        byte[] iv = base64toBytes(ivBase64);

        try {
            return new SeekableCipherInputStream(encryptedStream, encryptionKey, iv);
        } catch (Exception e) {
            throw new IOException("Error initializing decryption", e);
        }
    }
}


