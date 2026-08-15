using System.Security.Cryptography;
using System.Text;

namespace Orleans.SearchableStorage.Qualification.SkyPulse.Web;

internal static class CorpusGrowthAuthorization
{
    internal const string HeaderName = "X-SkyPulse-Corpus-Admin";

    internal static bool IsAuthorized(HttpRequest request, string expectedToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (string.IsNullOrEmpty(expectedToken)
            || !request.Headers.TryGetValue(HeaderName, out var values)
            || values.Count != 1
            || string.IsNullOrEmpty(values[0]))
        {
            return false;
        }

        var expected = Encoding.UTF8.GetBytes(expectedToken);
        var actual = Encoding.UTF8.GetBytes(values[0]!);
        var expectedDigest = SHA256.HashData(expected);
        var actualDigest = SHA256.HashData(actual);
        try
        {
            return CryptographicOperations.FixedTimeEquals(expectedDigest, actualDigest);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expected);
            CryptographicOperations.ZeroMemory(actual);
            CryptographicOperations.ZeroMemory(expectedDigest);
            CryptographicOperations.ZeroMemory(actualDigest);
        }
    }
}
