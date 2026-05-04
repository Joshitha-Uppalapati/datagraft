import { useEffect, useState } from "react";
import axios from "axios";

function ValidationPage({ fileId, onProceedToExport }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const runValidation = async () => {
      setLoading(true);
      setError(null);

      try {
        const res = await axios.get(
          `http://localhost:8000/api/validate/${fileId}`
        );
        setData(res.data);
      } catch (err) {
        setError(err?.response?.data?.detail || "Validation failed");
      } finally {
        setLoading(false);
      }
    };

    runValidation();
  }, [fileId]);

  if (loading) {
    return <p className="mt-6">Running validation...</p>;
  }

  if (error) {
    return (
      <div className="mt-6 p-4 bg-red-100 text-red-700 rounded">
        {error}
      </div>
    );
  }

  return (
    <div className="mt-6">
      <h2 className="text-xl font-semibold mb-4">Validation</h2>

      <div className="mb-6 grid grid-cols-3 gap-4">
        <div className="p-4 border rounded">
          <p className="text-sm text-gray-500">Total Rows</p>
          <p className="text-lg font-semibold">{data.total_rows}</p>
        </div>

        <div className="p-4 border rounded">
          <p className="text-sm text-gray-500">Clean Rows</p>
          <p className="text-lg font-semibold text-green-600">
            {data.clean_rows}
          </p>
        </div>

        <div className="p-4 border rounded">
          <p className="text-sm text-gray-500">Error Rows</p>
          <p className="text-lg font-semibold text-red-600">
            {data.error_rows}
          </p>
        </div>
      </div>

      {data.errors.length > 0 && (
        <table className="w-full text-left border-collapse">
          <thead className="bg-gray-50 border-b">
            <tr>
              <th className="px-4 py-2">Row</th>
              <th className="px-4 py-2">Column</th>
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Message</th>
            </tr>
          </thead>

          <tbody>
            {data.errors.map((err, idx) => (
              <tr key={idx} className="border-b">
                <td className="px-4 py-2">{err.row_index}</td>
                <td className="px-4 py-2">{err.column}</td>
                <td className="px-4 py-2">{err.error_type}</td>
                <td className="px-4 py-2">{err.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <button
        onClick={onProceedToExport}
        className="mt-6 rounded bg-slate-900 px-4 py-2 text-white"
      >
        Proceed to Export
      </button>
    </div>
  );
}

export default ValidationPage;