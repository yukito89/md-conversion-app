const fileInput = document.querySelector("#fileInput");
const status = document.querySelector("#status");
const uploadBtn = document.querySelector("#uploadBtn");

uploadBtn.addEventListener("click", async () => {
    const file = fileInput.files[0];

    if (!file) {
        status.textContent = "ファイルを選択してください";
        return;
    }

    uploadBtn.disabled = true;
    status.textContent = "生成中...";

    const formData = new FormData();
    formData.append("file", file);

    const endpoint = "http://localhost:7071/api/upload";

    try {
        const res = await fetch(endpoint, {
            method: "POST",
            body: formData,
        });

        console.log(res)

        if (!res.ok) {
            status.textContent = `エラー: ${res.status}`;
            uploadBtn.disabled = false;
            return;
        }

        const blob = await res.blob();
        const contentDisposition = res.headers.get('content-disposition');
        let filename = file.name.replace('.xlsx', '.md');
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="(.+)"/);
            if (match) filename = match[1];
        }

        const url = window.URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        a.remove();

        status.textContent = "完了しました";
    } catch (err) {
        status.textContent = `通信エラー: ${err.message}`;
    } finally {
        uploadBtn.disabled = false;
    }
});
