export const fetchUploadParts = async (upload) => {
  const fetchedParts = await Promise.all(upload.parts.map(async cid => {
    const url = new URL(`https://w3s.link/ipfs/${cid}`)
    const response = await fetch(url);
    return {
      type: 'part',
      cid,
      upload,
      url,
      response,
    }
  }))
  return fetchedParts
}