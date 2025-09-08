using System;
using System.Security.Cryptography;
using System.Text;

/// <summary>
/// Simple XOR-based encryptor for demonstration purposes only.
/// </summary>
public static class Encryptor {
    /// <summary>
    /// Encrypts or decrypts the buffer in-place using AES in CTR mode.
    /// </summary>
    /// <param name="buffer">The data buffer to encrypt or decrypt.</param>
    /// <param name="key">The AES key (must be a valid AES key length).</param>
    /// <param name="iv">The initialization vector (must be 16 bytes).</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="key"/> or <paramref name="iv"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown if <paramref name="iv"/> is not 16 bytes.</exception>
    public static void AES_CTR_inPlace(Span<byte> buffer, ReadOnlySpan<byte> key, ReadOnlySpan<byte> iv)
    {
        if (key.Length != 16 && key.Length != 24 && key.Length != 32)
            throw new ArgumentException("AES key must be 16, 24, or 32 bytes.", nameof(key));
        if (iv.Length != 16)
            throw new ArgumentException("IV must be 16 bytes.", nameof(iv));

        using var aes = Aes.Create();
        aes.Mode = CipherMode.ECB;        // CTR uses AES-ECB on the counter
        aes.Padding = PaddingMode.None;
        aes.Key = key.ToArray();

        using ICryptoTransform encryptor = aes.CreateEncryptor();

        byte[] counter = new byte[16];
        iv.CopyTo(counter);

        byte[] keystream = new byte[16];

        int offset = 0;
        while (offset < buffer.Length)
        {
            // AES(counter) -> keystream
            encryptor.TransformBlock(counter, 0, 16, keystream, 0);

            int n = Math.Min(16, buffer.Length - offset);
            for (int i = 0; i < n; i++)
            {
                buffer[offset + i] ^= keystream[i];
            }

            IncrementCounter(counter);
            offset += n;  // increment by what you actually processed
        }
    }

    private static void IncrementCounter(byte[] counter)
    {
        for (int i = 15; i >= 0; i--)
        {
            if (++counter[i] != 0) break;
        }
    }

    /// <summary>
    /// Generates a random 16-byte nonce using a secure random number generator.
    /// </summary>
    /// <returns>A randomly generated 16-byte nonce.</returns>
    public static byte[] GenerateNonce() {
        byte[] nonce = new byte[16];
        using RandomNumberGenerator rng = RandomNumberGenerator.Create();
        rng.GetBytes(nonce);
        return nonce;
    }
    
    /// <summary>
    /// Derives a cryptographic key from a password and salt using PBKDF2 with SHA-256.
    /// </summary>
    /// <param name="password">The password to derive the key from.</param>
    /// <param name="salt">The salt to use in the key derivation.</param>
    /// <param name="keySizeBytes">The desired key size in bytes (must be 16, 24, or 32).</param>
    /// <param name="iterations">The number of PBKDF2 iterations to perform (default is 200,000).</param>
    /// <returns>The derived key as a byte array.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="password"/> or <paramref name="salt"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="keySizeBytes"/> is not 16, 24, or 32.</exception>
    public static byte[] DeriveKeyPbkdf2(string password, byte[] salt, int keySizeBytes = 32, int iterations = 200_000)
    {
        if (password == null) throw new ArgumentNullException(nameof(password));
        if (salt == null) throw new ArgumentNullException(nameof(salt));
        if (keySizeBytes != 16 && keySizeBytes != 24 && keySizeBytes != 32)
            throw new ArgumentOutOfRangeException(nameof(keySizeBytes), "Must be 16, 24, or 32.");

        using var pbkdf2 = new Rfc2898DeriveBytes(
            password: password,
            salt: salt,
            iterations: iterations,
            hashAlgorithm: HashAlgorithmName.SHA256);

        return pbkdf2.GetBytes(keySizeBytes);
    }

    /// <summary>
    /// Generates a random salt of the specified size using a secure random number generator.
    /// </summary>
    /// <param name="size">The size of the salt in bytes. Default is 16.</param>
    /// <returns>A randomly generated salt byte array.</returns>
    public static byte[] GenerateSalt(int size = 16)
    {
        byte[] salt = new byte[size];
        RandomNumberGenerator.Fill(salt);
        return salt;
    }
}