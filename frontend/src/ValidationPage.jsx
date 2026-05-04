import { useState } from "react";
import axios from "axios";

function ValidationPage({ fileId, onValidationDone }) {
  const [result, setResult] = useState(null);

  const runValidation = async () => {
    const res = await axios.get(
      `http://localhost:8000/api/validate/${fileId}`
    );
    setResult(res.data);
  };

  return (
    <div>
      <h2>Validation</h2>

      <button onClick={runValidation}>Run Validation</button>

      {result && (
        <>
          <div>
            <p>Total Rows: {result.total_rows}</p>
            <p>Clean Rows: {result.clean_rows}</p>
            <p>Error Rows: {result.error_rows}</p>
          </div>

          <h3>Errors</h3>
          <table border="1" cellPadding="5">
            <thead>
              <tr>
                <th>Row</th>
                <th>Column</th>
                <th>Type</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {result.errors.map((e, i) => (
                <tr key={i}>
                  <td>{e.row_index}</td>
                  <td>{e.column}</td>
                  <td>{e.error_type}</td>
                  <td>{e.message}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <button onClick={onValidationDone}>
            Proceed to Export
          </button>
        </>
      )}
    </div>
  );
}

export default ValidationPage;