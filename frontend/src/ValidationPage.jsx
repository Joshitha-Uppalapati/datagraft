import { useEffect, useState } from "react";
import axios from "axios";

function ValidationPage({ fileId, onProceedToExport }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [errorTypeFilter, setErrorTypeFilter] = useState("all");

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
        setError(err?.response?.data?.detail || "Validation failed.");
      } finally {
        setLoading(false);
      }
    };

    runValidation();
  }, [fileId]);

  if (loading) {
    return <p className="mt-6 text-slate-600">Running validation...</p>;
  }

  if (error) {
    return (
      <div className="mt-6 rounded border border-red-300 bg-red-50 p-4 text-red-700">
        {error}
      </div>
    );
  }

  const errorTypes = [...new Set(data.errors.map((item) => item.error_type))];

  const filteredErrors =
    errorTypeFilter === "all"
      ? data.errors
      : data.errors.filter((item) => item.error_type === errorTypeFilter);

  return (
    <div className="mt-6">
      <h2 className="mb-4 text-xl font-semibold">Validation</h2>

      <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-3">
        <div className="rounded border p-4">
          <p className="text-sm text-gray-500">Total Rows</p>
          <p className="text-lg font-semibold">{data.total_rows}</p>
        </div>

        <div className="rounded border p-4">
          <p className="text-sm text-gray-500">Clean Rows</p>
          <p className="text-lg font-semibold text-green-600">
            {data.clean_rows}
          </p>
        </div>

        <div className="rounded border p-4">
          <p className="text-sm text-gray-500">Error Rows</p>
          <p className="text-lg font-semibold text-red-600">
            {data.error_rows}
          </p>
        </div>
      </div>

      {data.errors.length > 0 && (
        <div className="mb-4">
          <label className="mr-2 text-sm text-gray-600">
            Filter by error type:
          </label>

          <select
            value={errorTypeFilter}
            onChange={(event) => setErrorTypeFilter(event.target.value)}
            className="rounded border px-3 py-2"
          >
            <option value="all">All errors</option>
            {errorTypes.map((type) => (
              <option key={type} value={type}>
                {type}
              </option>
            ))}
          </select>
        </div>
      )}

      {filteredErrors.length > 0 ? (
        <table className="w-full border-collapse text-left">
          <thead className="border-b bg-gray-50">
            <tr>
              <th className="px-4 py-2">Row</th>
              <th className="px-4 py-2">Column</th>
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Message</th>
            </tr>
          </thead>

          <tbody>
            {filteredErrors.map((err, idx) => (
              <tr key={`${err.row_index}-${err.column}-${idx}`} className="border-b">
                <td className="px-4 py-2">{err.row_index}</td>
                <td className="px-4 py-2">{err.column}</td>
                <td className="px-4 py-2">{err.error_type}</td>
                <td className="px-4 py-2">{err.message}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p className="text-sm text-gray-600">
          No errors match this filter.
        </p>
      )}

      <button
        onClick={() => onProceedToExport(data)}
        className="mt-6 rounded bg-slate-900 px-4 py-2 text-white"
      >
        Proceed to Export
      </button>
    </div>
  );
}

export default ValidationPage;